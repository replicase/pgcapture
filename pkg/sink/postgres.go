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

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	pg_query "github.com/pganalyze/pg_query_go/v2"
	"github.com/rueian/pgcapture/pkg/decode"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/source"
	"github.com/rueian/pgcapture/pkg/sql"
	"github.com/sirupsen/logrus"
)

type PGXSink struct {
	BaseSink

	ConnStr   string
	SourceID  string
	Renice    int64
	LogReader io.Reader

	conn    *pgx.Conn
	raw     *pgconn.PgConn
	schema  *decode.PGXSchemaLoader
	log     *logrus.Entry
	prev    source.Checkpoint
	pgSrcID pgtype.Text
	replLag int64
	inTX    bool
	skip    map[string]bool
	inserts insertBatch
	prevDDL uint32
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

func tryRenice(logger *logrus.Entry, renice, pid int64) {
	out, err := exec.Command("renice", "-n", strconv.FormatInt(renice, 10), "-p", strconv.FormatInt(pid, 10)).CombinedOutput()
	logger.Infof("try renice to %d for pid %d: %s(%v)", renice, pid, string(out), err)
}

func (p *PGXSink) Setup() (cp source.Checkpoint, err error) {
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
	if err = p.schema.RefreshKeys(); err != nil {
		return cp, err
	}

	p.BaseSink.CleanFn = func() {
		p.conn.Close(context.Background())
	}

	p.log = logrus.WithFields(logrus.Fields{
		"From":     "PGXSink",
		"SourceID": p.SourceID,
	})

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

func (p *PGXSink) findCheckpoint(ctx context.Context) (cp source.Checkpoint, err error) {
	var str, ts string
	var mid []byte
	var seq uint32
	err = p.conn.QueryRow(ctx, "SELECT commit, seq, mid FROM pgcapture.sources WHERE id = $1 AND commit IS NOT NULL AND seq IS NOT NULL AND mid IS NOT NULL", p.SourceID).Scan(&str, &seq, &mid)
	if err == pgx.ErrNoRows {
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

func (p *PGXSink) Apply(changes chan source.Change) chan source.Checkpoint {
	var first bool
	return p.BaseSink.apply(changes, func(change source.Change, committed chan source.Checkpoint) (err error) {
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
				if err = p.rollback(); err != nil {
					break
				}
			}
			err = p.handleBegin(msg.Begin)
			p.inTX = true
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
				if err = p.handleCommit(change.Checkpoint, msg.Commit); err != nil {
					break
				}
			}
			committed <- change.Checkpoint
			p.inTX = false
			p.skip = nil
			p.prevDDL = 0
		}
		if err != nil {
			p.log.WithFields(logrus.Fields{
				"MessageLSN": change.Checkpoint.LSN,
				"MidHex":     hex.EncodeToString(change.Checkpoint.Data),
				"Message":    change.Message.String(),
			}).Errorf("fail to apply message: %v", err)
		}
		return err
	})
}

func (p *PGXSink) handleBegin(m *pb.Begin) (err error) {
	_, err = p.conn.Exec(context.Background(), "begin")
	return
}

func (p *PGXSink) rollback() (err error) {
	_, err = p.conn.Exec(context.Background(), "rollback")
	return
}

func (p *PGXSink) handleChange(m *pb.Change) (err error) {
	ctx := context.Background()
	switch m.Op {
	case pb.Change_INSERT:
		return p.handleInsert(ctx, m)
	case pb.Change_UPDATE:
		return p.handleUpdate(ctx, m)
	case pb.Change_DELETE:
		return p.handleDelete(ctx, m)
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
		err = p.performDDL(count, command)
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
	// remove preceding dml commands, they were already present in previous change messages
	// remove refresh materialized view stmt, otherwise they will block logical replication
	preceding := true
	for _, stmt := range tree.Stmts {
		preceding = preceding && (stmt.Stmt.GetInsertStmt() != nil || stmt.Stmt.GetUpdateStmt() != nil || stmt.Stmt.GetDeleteStmt() != nil)
		if preceding || stmt.Stmt.GetRefreshMatViewStmt() != nil {
			continue
		}
		stmts = append(stmts, stmt)
	}

	// record relations appeared in the statement, following change messages should be ignore if duplicated with them
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

func (p *PGXSink) performDDL(count int, ddl string) (err error) {
	ctx := context.Background()
	if count == 1 {
		// if the ddl contains only one command tag, than do not run it into begin/commit block
		// because command like "CREATE INDEX CONCURRENTLY" can't be run inside transaction
		if _, err = p.conn.Exec(ctx, "commit"); err != nil {
			return err
		}
	}
	if _, err = p.conn.Exec(ctx, ddl); err == nil && count == 1 {
		if _, err = p.conn.Exec(ctx, "begin"); err != nil {
			return err
		}
	}
	if err == nil {
		err = p.schema.RefreshKeys()
	}
	return
}

func (p *PGXSink) flushInsert(ctx context.Context) (err error) {
	batch := p.inserts.flush()
	if len(batch) == 0 {
		return nil
	}
	fields := len(batch[0])
	rets := make([]int16, fields)
	for i := 0; i < fields; i++ {
		rets[i] = 1
	}
	fmts := make([]int16, fields*len(batch))
	vals := make([][]byte, fields*len(batch))
	oids := make([]uint32, fields*len(batch))
	c := 0
	for i := 0; i < len(batch); i++ {
		for j := 0; j < fields; j++ {
			field := batch[i][j]
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

	keys, _ := p.schema.GetTableKey(p.inserts.Schema, p.inserts.Table)

	return p.raw.ExecParams(ctx, sql.InsertQuery(p.inserts.Schema, p.inserts.Table, keys, batch[0], len(batch)), vals, oids, fmts, rets).Read().Err
}

func (p *PGXSink) handleInsert(ctx context.Context, m *pb.Change) (err error) {
	if !p.inserts.ok(m) {
		if err = p.flushInsert(ctx); err != nil {
			return err
		}
	}
	p.inserts.push(m)
	return nil
}

func (p *PGXSink) handleDelete(ctx context.Context, m *pb.Change) (err error) {
	if err = p.flushInsert(ctx); err != nil {
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
	return p.raw.ExecParams(ctx, sql.DeleteQuery(m.Schema, m.Table, m.Old), vals, oids, fmts, fmts).Read().Err
}

func (p *PGXSink) handleUpdate(ctx context.Context, m *pb.Change) (err error) {
	if err = p.flushInsert(ctx); err != nil {
		return err
	}

	var keys []*pb.Field
	var sets []*pb.Field
	if m.Old != nil {
		keys = m.Old
		sets = m.New
	} else {
		kf, err := p.schema.GetTableKey(m.Schema, m.Table)
		if err != nil {
			return err
		}
		keys = make([]*pb.Field, 0, len(kf))
		sets = make([]*pb.Field, 0, len(m.New)-len(kf))
	nextField:
		for _, f := range m.New {
			for _, k := range kf {
				if k == f.Name {
					keys = append(keys, f)
					continue nextField
				}
			}
			sets = append(sets, f)
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
		j = i + j
		field := keys[i]
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

	return p.raw.ExecParams(ctx, sql.UpdateQuery(m.Schema, m.Table, sets, keys), vals, oids, fmts, fmts).Read().Err
}

const (
	UpdateSourceSQL = "update pgcapture.sources set commit=$1,seq=$2,mid=$3,commit_ts=$4,apply_ts=now() where id=$5"
)

func (p *PGXSink) handleCommit(cp source.Checkpoint, commit *pb.Commit) (err error) {
	ctx := context.Background()
	if err = p.flushInsert(ctx); err != nil {
		return err
	}
	if _, err = p.conn.Prepare(ctx, UpdateSourceSQL, UpdateSourceSQL); err != nil {
		return err
	}
	commitTs := pgTz(commit.CommitTime)
	if _, err = p.conn.Exec(ctx, UpdateSourceSQL, pgInt8(int64(cp.LSN)), pgInt4(int32(cp.Seq)), cp.Data, commitTs, p.pgSrcID); err != nil {
		return err
	}
	if _, err = p.conn.Exec(ctx, "commit"); err != nil {
		return err
	}
	atomic.StoreInt64(&p.replLag, time.Since(commitTs.Time).Milliseconds())
	return
}

func (p *PGXSink) ReplicationLagMilliseconds() int64 {
	return atomic.LoadInt64(&p.replLag)
}

func ScanCheckpointFromLog(f io.Reader) (lsn, ts string, err error) {
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if match := LogLSNRegex.FindStringSubmatch(line); len(match) > 1 {
			lsn = match[1]
		} else if match = LogTxTimeRegex.FindStringSubmatch(line); len(match) > 1 {
			ts = match[1]
		}
	}
	if lsn == "" || ts == "" {
		return "", "", fmt.Errorf("lsn not found in log: %w", scanner.Err())
	}
	return lsn, ts, nil
}

var (
	LogLSNRegex    = regexp.MustCompile(`(?:consistent recovery state reached at|redo done at) ([0-9A-F]{1,8}\/[0-9A-F]{1,8})`)
	LogTxTimeRegex = regexp.MustCompile(`last completed transaction was at log time (.*)\.?$`)
)

func pgText(t string) pgtype.Text {
	return pgtype.Text{String: t, Status: pgtype.Present}
}

func pgInt8(i int64) pgtype.Int8 {
	return pgtype.Int8{Int: i, Status: pgtype.Present}
}

func pgInt4(i int32) pgtype.Int4 {
	return pgtype.Int4{Int: i, Status: pgtype.Present}
}

func pgTz(ts uint64) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: PGTime2Time(ts), Status: pgtype.Present}
}

func PGTime2Time(ts uint64) time.Time {
	micro := microsecFromUnixEpochToY2K + int64(ts)
	return time.Unix(micro/microInSecond, (micro%microInSecond)*nsInSecond)
}

const microInSecond = int64(1e6)
const nsInSecond = int64(1e3)
const microsecFromUnixEpochToY2K = int64(946684800 * 1000000)
