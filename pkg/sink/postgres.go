package sink

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	pg_query "github.com/pganalyze/pg_query_go/v2"
	"github.com/replicase/pgcapture/pkg/cursor"
	"github.com/replicase/pgcapture/pkg/decode"
	"github.com/replicase/pgcapture/pkg/pb"
	"github.com/replicase/pgcapture/pkg/source"
	"github.com/replicase/pgcapture/pkg/sql"
	"github.com/sirupsen/logrus"
)

type PGXSink struct {
	BaseSink

	ConnStr     string
	SourceID    string
	Renice      int64
	LogReader   io.Reader
	BatchTXSize int

	conn           *pgx.Conn
	raw            *pgconn.PgConn
	pipeline       *pgconn.Pipeline
	schema         *decode.PGXSchemaLoader
	log            *logrus.Entry
	prev           cursor.Checkpoint
	pgSrcID        pgtype.Text
	pgVersion      int64
	replLag        int64
	skip           map[string]bool
	inserts        insertBatch
	prevDDL        uint32
	inTX           bool
	pendingChanges []pendingChange
	pendingCommits []pendingCommit
}

type insertBatch struct {
	Schema  string
	Table   string
	records [][]*pb.Field
	offset  int
}

func (b *insertBatch) ok(m *pb.Change) bool {
	return b.offset != len(b.records) && b.Table == m.Table && b.Schema == m.Schema
}

func (b *insertBatch) push(m *pb.Change) {
	b.Table = m.Table
	b.Schema = m.Schema
	b.records[b.offset] = m.New
	b.offset++
}

func (b *insertBatch) flush() [][]*pb.Field {
	records := b.records[:b.offset]
	b.offset = 0
	return records
}

type pendingChange struct {
	sql           string
	args          [][]byte
	paramOIDs     []uint32
	paramFormats  []int16
	resultFormats []int16
}

type pendingCommit struct {
	checkPoint cursor.Checkpoint
	commit     *pb.Commit
}

func tryRenice(logger *logrus.Entry, renice, pid int64) {
	out, err := exec.Command("renice", "-n", strconv.FormatInt(renice, 10), "-p", strconv.FormatInt(pid, 10)).CombinedOutput()
	logger.Infof("try renice to %d for pid %d: %s(%v)", renice, pid, string(out), err)
}

func (p *PGXSink) Setup() (cp cursor.Checkpoint, err error) {
	ctx := context.Background()
	p.conn, err = pgx.Connect(ctx, p.ConnStr)
	if err != nil {
		return cp, err
	}
	p.raw = p.conn.PgConn()
	p.inserts.records = make([][]*pb.Field, 2500)
	p.pgSrcID = pgText(p.SourceID)
	p.replLag = -1

	var locked bool
	if err := p.conn.QueryRow(ctx, "select pg_try_advisory_lock(('x' || md5(current_database()))::bit(64)::bigint)").Scan(&locked); err != nil {
		return cp, err
	}
	if !locked {
		return cp, errors.New("pg_try_advisory_lock failed, another process is occupying")
	}

	if _, err = p.conn.Exec(ctx, sql.InstallExtension); err != nil {
		return cp, err
	}

	if _, err = p.conn.Exec(ctx, "insert into pgcapture.sources(id) values ($1) on conflict (id) do nothing", p.pgSrcID); err != nil {
		return cp, err
	}

	p.schema = decode.NewPGXSchemaLoader(p.conn)
	if err = p.schema.RefreshColumnInfo(); err != nil {
		return cp, err
	}

	p.pgVersion, err = p.schema.GetVersion()
	if err != nil {
		return cp, err
	}

	p.BaseSink.CleanFn = func() {
		p.conn.Close(context.Background())
	}

	p.log = logrus.WithFields(logrus.Fields{
		"From":        "PGXSink",
		"SourceID":    p.SourceID,
		"BatchTxSize": p.BatchTXSize,
	})

	p.pendingCommits = make([]pendingCommit, 0, p.BatchTXSize)

	// try renice
	if p.Renice < 0 {
		var pid int64
		if err := p.conn.QueryRow(ctx, "select pg_backend_pid()").Scan(&pid); err != nil {
			return cp, err
		}
		tryRenice(p.log, p.Renice, pid)
		tryRenice(p.log, p.Renice, int64(os.Getpid()))
	}

	return p.findCheckpoint(ctx)
}

func (p *PGXSink) findCheckpoint(ctx context.Context) (cp cursor.Checkpoint, err error) {
	var str, ts string
	var mid []byte
	var seq uint32
	err = p.conn.QueryRow(ctx, "SELECT commit, seq, mid FROM pgcapture.sources WHERE id = $1 AND commit IS NOT NULL AND seq IS NOT NULL AND mid IS NOT NULL", p.SourceID).Scan(&str, &seq, &mid)
	if errors.Is(err, pgx.ErrNoRows) {
		err = nil
		if p.LogReader != nil {
			p.log.Info("try to find last checkpoint from log reader")
			str, ts, err = ScanCheckpointFromLog(p.LogReader)
		}
	}
	if str != "" {
		var l pglogrepl.LSN
		l, err = pglogrepl.ParseLSN(str)
		cp.LSN = uint64(l)
		cp.Seq = seq
	}
	if len(mid) != 0 {
		cp.Data = mid
		p.log.WithFields(logrus.Fields{
			"RequiredLSN": cp.LSN,
			"MidHex":      hex.EncodeToString(mid),
		}).Info("last checkpoint found")
	} else if len(ts) != 0 {
		var pts time.Time
		if pts, err = time.Parse("2006-01-02 15:04:05.999999999Z07", ts); err == nil {
			cp.Data = []byte(pts.Format(time.RFC3339Nano))
			p.log.WithFields(logrus.Fields{
				"RequiredLSN": cp.LSN,
				"SeekTs":      pts,
			}).Info("last checkpoint found")
		}
	}
	if err != nil {
		return cp, err
	}

	if cp.LSN != 0 {
		p.prev = cp
	}
	return cp, nil
}

func (p *PGXSink) Apply(changes chan source.Change) chan cursor.Checkpoint {
	var first bool
	return p.BaseSink.apply(changes, func(sourceRemaining int, change source.Change, committed chan cursor.Checkpoint) (err error) {
		if !first {
			p.log.WithFields(logrus.Fields{
				"MessageLSN":  change.Checkpoint.LSN,
				"SinkLastLSN": p.prev.LSN,
				"Message":     change.Message.String(),
			}).Info("applying the first message from source")
			first = true
		}

		switch msg := change.Message.Type.(type) {
		case *pb.Message_Begin:
			if p.inTX {
				p.log.WithFields(logrus.Fields{
					"MessageLSN": change.Checkpoint.LSN,
					"MidHex":     hex.EncodeToString(change.Checkpoint.Data),
				}).Warn("receive incomplete transaction: begin")
				p.pendingChanges = p.pendingChanges[:0]
				break
			}
			p.inTX = true
			p.handleBegin(msg.Begin)
		case *pb.Message_Change:
			if !p.inTX {
				p.log.WithFields(logrus.Fields{
					"MessageLSN": change.Checkpoint.LSN,
					"MidHex":     hex.EncodeToString(change.Checkpoint.Data),
				}).Warn("receive incomplete transaction: change")
				break
			}
			if decode.IsDDL(msg.Change) {
				err = p.handleDDL(msg.Change)
			} else {
				if len(p.skip) != 0 && p.skip[fmt.Sprintf("%s.%s", msg.Change.Schema, msg.Change.Table)] {
					p.log.WithFields(logrus.Fields{
						"MessageLSN": change.Checkpoint.LSN,
						"MidHex":     hex.EncodeToString(change.Checkpoint.Data),
						"Message":    change.Message.String(),
					}).Warn("message skipped due to previous commands mixed with DML and DDL")
					return nil
				}
				err = p.handleChange(msg.Change)
			}
		case *pb.Message_Commit:
			if !p.inTX {
				p.log.WithFields(logrus.Fields{
					"MessageLSN": change.Checkpoint.LSN,
					"MidHex":     hex.EncodeToString(change.Checkpoint.Data),
				}).Warn("receive incomplete transaction: commit")
			} else {
				if err = p.handleCommit(sourceRemaining, change.Checkpoint, msg.Commit); err != nil {
					break
				}
			}
			p.inTX = false
			p.skip = nil
			p.prevDDL = 0
		}

		if err != nil {
			p.log.WithFields(logrus.Fields{
				"MessageLSN":     change.Checkpoint.LSN,
				"MidHex":         hex.EncodeToString(change.Checkpoint.Data),
				"Message":        change.Message.String(),
				"PendingCommits": p.pendingCommits,
			}).Errorf("fail to apply message: %v", err)
		}
		return err
	})
}

func (p *PGXSink) handleBegin(b *pb.Begin) {
	p.startPipeline()
}

func (p *PGXSink) handleChange(m *pb.Change) (err error) {
	switch m.Op {
	case pb.Change_INSERT:
		return p.handleInsert(m)
	case pb.Change_UPDATE:
		return p.handleUpdate(m)
	case pb.Change_DELETE:
		return p.handleDelete(m)
	}
	return nil
}

func (p *PGXSink) handleDDL(m *pb.Change) (err error) {
	command, relations, count, err := p.parseDDL(m.New)
	if err != nil {
		return err
	}
	if count == 0 {
		return nil
	}

	checksum := crc32.ChecksumIEEE([]byte(command))
	if p.prevDDL != checksum {
		p.skip = relations
		err = p.performDDL(command)
	}
	p.prevDDL = checksum

	return err
}

func (p *PGXSink) parseDDL(fields []*pb.Field) (command string, relations map[string]bool, count int, err error) {
	for _, field := range fields {
		switch field.Name {
		case "query":
			command = string(field.GetBinary())
			goto parse
		}
	}

parse:
	tree, err := pg_query.Parse(command)
	if err != nil {
		return "", nil, 0, err
	}

	var stmts []*pg_query.RawStmt
	// remove refresh materialized view stmt, otherwise they will block logical replication
	for _, stmt := range tree.Stmts {
		if stmt.Stmt.GetRefreshMatViewStmt() != nil {
			continue
		}
		stmts = append(stmts, stmt)
	}

	// record relations appeared in the statement, following change messages should be ignored if duplicated with them
	relations = make(map[string]bool)
	for _, stmt := range stmts {
		var relation *pg_query.RangeVar
		switch node := stmt.Stmt.Node.(type) {
		case *pg_query.Node_InsertStmt:
			relation = node.InsertStmt.Relation
		case *pg_query.Node_UpdateStmt:
			relation = node.UpdateStmt.Relation
		case *pg_query.Node_DeleteStmt:
			relation = node.DeleteStmt.Relation
		case *pg_query.Node_CreateTableAsStmt:
			relation = node.CreateTableAsStmt.Into.Rel
		case *pg_query.Node_SelectStmt:
			if node.SelectStmt.IntoClause != nil {
				relation = node.SelectStmt.IntoClause.Rel
			}
		}
		if relation == nil {
			continue
		}
		if relation.Schemaname == "" {
			relations[fmt.Sprintf("public.%s", relation.Relname)] = true
		} else {
			relations[fmt.Sprintf("%s.%s", relation.Schemaname, relation.Relname)] = true
		}
	}

	sb := strings.Builder{}
	for _, stmt := range stmts {
		if stmt.StmtLen == 0 {
			sb.WriteString(command[stmt.StmtLocation:])
		} else {
			sb.WriteString(command[stmt.StmtLocation : stmt.StmtLocation+stmt.StmtLen])
		}
		sb.WriteString(";")
	}

	return sb.String(), relations, len(stmts), nil
}

func (p *PGXSink) performDDL(ddl string) (err error) {
	p.pendingChanges = p.pendingChanges[:0]
	if err = p.endPipeline(); err != nil {
		return err
	}
	if _, err = p.conn.Exec(context.Background(), ddl); err != nil {
		return err
	}
	if err = p.schema.RefreshColumnInfo(); err != nil {
		return err
	}
	p.startPipeline()
	return
}

func (p *PGXSink) flushInsert() (err error) {
	batch := p.inserts.flush()
	if len(batch) == 0 {
		return nil
	}

	info, _ := p.schema.GetColumnInfo(p.inserts.Schema, p.inserts.Table)
	cols, filtered := info.Filter(batch[0], func(i decode.ColumnInfo, field string) bool {
		return !i.IsGenerated(field)
	})

	fCount := len(filtered)
	rets := make([]int16, fCount)
	for i := 0; i < fCount; i++ {
		rets[i] = 1
	}
	fmts := make([]int16, fCount*len(batch))
	vals := make([][]byte, fCount*len(batch))
	oids := make([]uint32, fCount*len(batch))
	c := 0
	for i := 0; i < len(batch); i++ {
		for j := 0; j < len(batch[0]); j++ {
			field := batch[i][j]
			if !cols.Contains(field.Name) {
				// skip the data since it's the generated column
				continue
			}
			if field.Value == nil {
				fmts[c] = 1
				vals[c] = nil
				oids[c] = field.Oid
			} else if value, ok := field.Value.(*pb.Field_Binary); ok {
				fmts[c] = 1
				vals[c] = value.Binary
				oids[c] = field.Oid
			} else {
				fmts[c] = 0
				vals[c] = []byte(field.GetText())
				oids[c] = 0
			}
			c++
		}
	}
	fmts = fmts[:c]
	vals = vals[:c]
	oids = oids[:c]

	keys := info.ListKeys()
	opt := sql.InsertOption{
		Namespace: p.inserts.Schema,
		Table:     p.inserts.Table,
		Keys:      keys,
		Fields:    filtered,
		Count:     len(batch),
		PGVersion: p.pgVersion,
	}

	p.pendingChanges = append(p.pendingChanges, pendingChange{
		sql:           sql.InsertQuery(opt),
		args:          vals,
		paramOIDs:     oids,
		paramFormats:  fmts,
		resultFormats: rets,
	})
	return
}

func (p *PGXSink) handleInsert(m *pb.Change) (err error) {
	if !p.inserts.ok(m) {
		if err = p.flushInsert(); err != nil {
			return err
		}
	}
	p.inserts.push(m)
	return nil
}

func (p *PGXSink) handleDelete(m *pb.Change) (err error) {
	if err = p.flushInsert(); err != nil {
		return err
	}

	fields := len(m.Old)
	vals := make([][]byte, fields)
	oids := make([]uint32, fields)
	fmts := make([]int16, fields)
	for i := 0; i < fields; i++ {
		field := m.Old[i]
		if field.Value == nil {
			fmts[i] = 1
			vals[i] = nil
			oids[i] = field.Oid
		} else if value, ok := field.Value.(*pb.Field_Binary); ok {
			fmts[i] = 1
			vals[i] = value.Binary
			oids[i] = field.Oid
		} else {
			fmts[i] = 0
			vals[i] = []byte(field.GetText())
			oids[i] = 0
		}
	}
	p.pendingChanges = append(p.pendingChanges, pendingChange{
		sql:           sql.DeleteQuery(m.Schema, m.Table, m.Old),
		args:          vals,
		paramOIDs:     oids,
		paramFormats:  fmts,
		resultFormats: fmts,
	})
	return
}

func (p *PGXSink) handleUpdate(m *pb.Change) (err error) {
	if err = p.flushInsert(); err != nil {
		return err
	}

	info, err := p.schema.GetColumnInfo(m.Schema, m.Table)
	if err != nil {
		return err
	}

	var (
		keys []*pb.Field
		sets []*pb.Field
	)
	if m.Old != nil {
		keys = m.Old
		_, sets = info.Filter(m.New, func(i decode.ColumnInfo, field string) bool {
			return !i.IsGenerated(field) && !i.IsIdentityGeneration(field)
		})
	} else {
		ksize := info.KeyLength()
		keys = make([]*pb.Field, 0, ksize)
		sets = make([]*pb.Field, 0, len(m.New)-ksize)
		for _, f := range m.New {
			fname := f.Name
			if info.IsKey(fname) {
				keys = append(keys, f)
			} else if !info.IsGenerated(fname) && !info.IsIdentityGeneration(fname) {
				sets = append(sets, f)
			}
		}
	}

	if len(sets) == 0 || len(keys) == 0 {
		return nil
	}

	fields := len(sets) + len(keys)
	vals := make([][]byte, fields)
	oids := make([]uint32, fields)
	fmts := make([]int16, fields)

	var j int
	for ; j < len(sets); j++ {
		field := sets[j]
		if field.Value == nil {
			fmts[j] = 1
			vals[j] = nil
			oids[j] = field.Oid
		} else if value, ok := field.Value.(*pb.Field_Binary); ok {
			fmts[j] = 1
			vals[j] = value.Binary
			oids[j] = field.Oid
		} else {
			fmts[j] = 0
			vals[j] = []byte(field.GetText())
			oids[j] = 0
		}
	}

	for i := 0; i < len(keys); i++ {
		k := i + j
		field := keys[i]
		if field.Value == nil {
			fmts[k] = 1
			vals[k] = nil
			oids[k] = field.Oid
		} else if value, ok := field.Value.(*pb.Field_Binary); ok {
			fmts[k] = 1
			vals[k] = value.Binary
			oids[k] = field.Oid
		} else {
			fmts[k] = 0
			vals[k] = []byte(field.GetText())
			oids[k] = 0
		}
	}

	p.pendingChanges = append(p.pendingChanges, pendingChange{
		sql:           sql.UpdateQuery(m.Schema, m.Table, sets, keys),
		args:          vals,
		paramOIDs:     oids,
		paramFormats:  fmts,
		resultFormats: fmts,
	})
	return
}

const (
	UpdateSourceSQL = "update pgcapture.sources set commit=$1,seq=$2,mid=$3,commit_ts=$4,apply_ts=now() where id=$5"
)

func (p *PGXSink) handleCommit(sourceRemaining int, cp cursor.Checkpoint, commit *pb.Commit) (err error) {
	if err = p.flushInsert(); err != nil {
		return err
	}

	for _, q := range p.pendingChanges {
		p.pipeline.SendQueryParams(q.sql, q.args, q.paramOIDs, q.paramFormats, q.resultFormats)
	}
	p.pendingChanges = p.pendingChanges[:0]
	p.pendingCommits = append(p.pendingCommits, pendingCommit{
		checkPoint: cp,
		commit:     commit,
	})

	if len(p.pendingCommits) == p.BatchTXSize || sourceRemaining == 0 {
		var (
			cmt   []byte
			seq   []byte
			mid   []byte
			cmtTs []byte
			id    []byte
		)

		// pgtype does not support uint64, so we have to encode it as text
		cmt, err = p.conn.TypeMap().Encode(0, pgtype.TextFormatCode, pgLSN(cp.LSN), nil)
		if err != nil {
			return err
		}
		seq, err = p.conn.TypeMap().Encode(pgtype.Int4OID, pgtype.BinaryFormatCode, pgInt4(int32(cp.Seq)), nil)
		if err != nil {
			return err
		}
		mid, err = p.conn.TypeMap().Encode(pgtype.ByteaOID, pgtype.BinaryFormatCode, cp.Data, nil)
		if err != nil {
			return err
		}
		cmtTs, err = p.conn.TypeMap().Encode(pgtype.TimestamptzOID, pgtype.BinaryFormatCode, pgTz(commit.CommitTime), nil)
		if err != nil {
			return err
		}
		id, err = p.conn.TypeMap().Encode(pgtype.TextOID, pgtype.BinaryFormatCode, p.pgSrcID, nil)
		if err != nil {
			return err
		}
		p.pipeline.SendQueryParams(UpdateSourceSQL, [][]byte{cmt, seq, mid, cmtTs, id}, []uint32{0, pgtype.Int4OID, pgtype.ByteaOID, pgtype.TimestamptzOID, pgtype.TextOID}, []int16{pgtype.TextFormatCode, pgtype.BinaryFormatCode, pgtype.BinaryFormatCode, pgtype.BinaryFormatCode, pgtype.BinaryFormatCode}, []int16{pgtype.TextFormatCode, pgtype.BinaryFormatCode, pgtype.BinaryFormatCode, pgtype.BinaryFormatCode, pgtype.BinaryFormatCode})
		err = p.endPipeline()
	}
	return
}

func (p *PGXSink) startPipeline() {
	if p.pipeline == nil {
		p.pipeline = p.raw.StartPipeline(context.Background())
	}
}

func (p *PGXSink) endPipeline() (err error) {
	if err = p.pipeline.Sync(); err != nil {
		return err
	}
	if err = p.pipeline.Close(); err != nil {
		return err
	}
	if len(p.pendingCommits) != 0 {
		atomic.StoreInt64(&p.replLag, time.Since(pgTz(p.pendingCommits[len(p.pendingCommits)-1].commit.CommitTime).Time).Milliseconds())
	}
	p.pipeline = nil
	for _, commit := range p.pendingCommits {
		p.committed <- commit.checkPoint
	}
	p.pendingCommits = p.pendingCommits[:0]
	return
}

func (p *PGXSink) ReplicationLagMilliseconds() int64 {
	return atomic.LoadInt64(&p.replLag)
}

func ScanCheckpointFromLog(f io.Reader) (lsn, ts string, err error) {
	reader := bufio.NewReader(f)
	for {
		var line []byte
		if line, _, err = reader.ReadLine(); err != nil {
			break
		}
		str := *(*string)(unsafe.Pointer(&line))
		if match := LogLSNRegex.FindStringSubmatch(str); len(match) > 1 {
			lsn = clone(match[1])
		} else if match = LogTxTimeRegex.FindStringSubmatch(str); len(match) > 1 {
			ts = clone(match[1])
		}
	}
	if lsn == "" && ts == "" {
		return "", "", fmt.Errorf("lsn not found in log: %w", err)
	}
	return lsn, ts, nil
}

var (
	LogLSNRegex    = regexp.MustCompile(`(?:consistent recovery state reached at|redo done at) ([0-9A-F]{1,8}\/[0-9A-F]{1,8})`)
	LogTxTimeRegex = regexp.MustCompile(`last completed transaction was at log time (.*)\.?$`)
)

func pgText(t string) pgtype.Text {
	return pgtype.Text{String: t, Valid: true}
}

func pgLSN(i uint64) pglogrepl.LSN {
	return pglogrepl.LSN(i)
}

func pgInt4(i int32) pgtype.Int4 {
	return pgtype.Int4{Int32: i, Valid: true}
}

func pgTz(ts uint64) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: PGTime2Time(ts), Valid: true}
}

func PGTime2Time(ts uint64) time.Time {
	micro := microsecFromUnixEpochToY2K + int64(ts)
	return time.Unix(micro/microInSecond, (micro%microInSecond)*nsInSecond)
}

func clone(s string) string {
	b := make([]byte, len(s))
	copy(b, s)
	return *(*string)(unsafe.Pointer(&b))
}

const microInSecond = int64(1e6)
const nsInSecond = int64(1e3)
const microsecFromUnixEpochToY2K = int64(946684800 * 1000000)
