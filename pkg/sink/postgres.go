package sink

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/rueian/pgcapture/pkg/decode"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/sql"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type PGXSink struct {
	ConnStr  string
	LogPath  string
	SourceID string

	conn   *pgx.Conn
	raw    *pgconn.PgConn
	schema *decode.PGXSchemaLoader

	err     error
	inTX    bool
	refresh bool
	stopped uint64
}

func (p *PGXSink) Setup() (lsn uint64, err error) {
	ctx := context.Background()
	p.conn, err = pgx.Connect(ctx, p.ConnStr)
	if err != nil {
		return 0, err
	}
	p.raw = p.conn.PgConn()

	if _, err = p.conn.Exec(ctx, sql.InstallExtension); err != nil {
		return 0, err
	}

	p.schema = decode.NewPGXSchemaLoader(p.conn)
	if err = p.schema.RefreshKeys(); err != nil {
		return 0, err
	}

	return p.findCommittedLSN(ctx)
}

func (p *PGXSink) findCommittedLSN(ctx context.Context) (lsn uint64, err error) {
	var str string
	err = p.conn.QueryRow(ctx, "SELECT commit FROM pgcapture.sources WHERE id = $1 AND message IS NULL", p.SourceID).Scan(&str)
	if err == pgx.ErrNoRows && p.LogPath != "" {
		err = nil
		if p.LogPath != "" {
			str, err = ScanLSNFromLog(p.LogPath)
		}
	}
	if str != "" {
		var l pglogrepl.LSN
		l, err = pglogrepl.ParseLSN(str)
		lsn = uint64(l)
	}
	if err != nil {
		return 0, err
	}
	return lsn, nil
}

func (p *PGXSink) Apply(changes chan *pb.Message) (committed chan uint64) {
	committed = make(chan uint64, 100)
	go func() {
		var cleaned bool
		check := func() bool {
			if atomic.LoadUint64(&p.stopped) != 1 {
				return true
			}
			if !cleaned {
				close(committed)
				p.conn.Close(context.Background())
				cleaned = true
			}
			return false
		}
		for {
			select {
			case change, more := <-changes:
				if !more {
					return
				}
				if !check() {
					continue
				}
				switch change.Type.(type) {
				case *pb.Message_Begin:
					p.err = p.handleBegin(change.GetBegin())
					p.inTX = true
				case *pb.Message_Change:
					if p.inTX {
						m := change.GetChange()
						if decode.IsDDL(m) {
							p.refresh = true
							p.err = p.handleDDL(m)
						} else {
							p.err = p.handleChange(m)
						}
					} else {
						p.err = errors.New("receive incomplete transaction")
					}
				case *pb.Message_Commit:
					p.err = p.handleCommit(change.GetCommit(), committed)
					p.inTX = false
					if p.refresh && p.err == nil {
						p.err = p.schema.RefreshKeys()
						p.refresh = false
					}
				}
				if p.err != nil {
					p.Stop() // mark stop, but do not return
				}
			default:
				check()
				time.Sleep(time.Millisecond * 100)
			}
		}
	}()
	return
}

func (p *PGXSink) Error() error {
	return p.err
}

func (p *PGXSink) Stop() {
	atomic.StoreUint64(&p.stopped, 1)
}

func (p *PGXSink) handleBegin(m *pb.Begin) (err error) {
	_, err = p.conn.Exec(context.Background(), "begin")
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
	for _, field := range m.NewTuple {
		if field.Name == "query" {
			_, err = p.conn.Exec(context.Background(), string(field.Datum))
			return err
		}
	}
	return nil
}

func (p *PGXSink) handleInsert(ctx context.Context, m *pb.Change) (err error) {
	fields := len(m.NewTuple)
	vals := make([][]byte, fields)
	oids := make([]uint32, fields)
	fmts := make([]int16, fields)
	ci := p.conn.ConnInfo()

	var query strings.Builder
	query.WriteString("insert into \"")
	query.WriteString(m.Namespace)
	query.WriteString("\".\"")
	query.WriteString(m.Table)
	query.WriteString("\"(\"")
	for i := 0; i < fields; i++ {
		field := m.NewTuple[i]
		vals[i] = field.Datum
		oids[i] = field.Oid
		fmts[i] = ci.ParamFormatCodeForOID(field.Oid)
		query.WriteString(field.Name)
		if i == fields-1 {
			query.WriteString("\") values (")
		} else {
			query.WriteString("\",\"")
		}
	}
	for i := 1; i <= fields; i++ {
		query.WriteString("$" + strconv.Itoa(i))
		if i == fields {
			query.WriteString(")")
		} else {
			query.WriteString(",")
		}
	}
	return p.raw.ExecParams(ctx, query.String(), vals, oids, fmts, fmts).Read().Err
}

func (p *PGXSink) handleDelete(ctx context.Context, m *pb.Change) (err error) {
	fields := len(m.OldTuple)
	vals := make([][]byte, fields)
	oids := make([]uint32, fields)
	fmts := make([]int16, fields)
	ci := p.conn.ConnInfo()

	var query strings.Builder
	query.WriteString("delete from \"")
	query.WriteString(m.Namespace)
	query.WriteString("\".\"")
	query.WriteString(m.Table)
	query.WriteString("\" where \"")

	for i := 0; i < fields; i++ {
		field := m.OldTuple[i]
		vals[i] = field.Datum
		oids[i] = field.Oid
		fmts[i] = ci.ParamFormatCodeForOID(field.Oid)

		query.WriteString(field.Name)
		query.WriteString("\"=$" + strconv.Itoa(i+1))
		if i != fields-1 {
			query.WriteString(" AND \"")
		}
	}
	return p.raw.ExecParams(ctx, query.String(), vals, oids, fmts, fmts).Read().Err
}

func (p *PGXSink) handleUpdate(ctx context.Context, m *pb.Change) (err error) {
	var keys []*pb.Field
	var sets []*pb.Field
	if m.OldTuple != nil {
		keys = m.OldTuple
		sets = m.NewTuple
	} else {
		kf, err := p.schema.GetTableKey(m.Namespace, m.Table)
		if err != nil {
			return err
		}
		keys = make([]*pb.Field, 0, len(kf))
		sets = make([]*pb.Field, 0, len(m.NewTuple)-len(kf))
	nextField:
		for _, f := range m.NewTuple {
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
	ci := p.conn.ConnInfo()

	var query strings.Builder
	query.WriteString("update \"")
	query.WriteString(m.Namespace)
	query.WriteString("\".\"")
	query.WriteString(m.Table)
	query.WriteString("\" set \"")

	var j int
	for ; j < len(sets); j++ {
		field := sets[j]
		vals[j] = field.Datum
		oids[j] = field.Oid
		fmts[j] = ci.ParamFormatCodeForOID(field.Oid)

		query.WriteString(field.Name)
		query.WriteString("\"=$" + strconv.Itoa(j+1))
		if j != len(sets)-1 {
			query.WriteString(",\"")
		}
	}

	query.WriteString("where \"")

	for i := 0; i < len(keys); i++ {
		j = i + j
		field := keys[i]
		vals[j] = field.Datum
		oids[j] = field.Oid
		fmts[j] = ci.ParamFormatCodeForOID(field.Oid)

		query.WriteString(field.Name)
		query.WriteString("\"=$" + strconv.Itoa(j+1))
		if i != len(keys)-1 {
			query.WriteString(" AND \"")
		}
	}

	return p.raw.ExecParams(ctx, query.String(), vals, oids, fmts, fmts).Read().Err
}

const (
	UpdateSourceSQL = "insert into pgcapture.sources(id,commit,commit_ts) values ($1,$2,$3) on conflict (id) do update set commit=EXCLUDED.commit,commit_ts=EXCLUDED.commit_ts,apply_ts=now()"
)

func (p *PGXSink) handleCommit(m *pb.Commit, committed chan uint64) (err error) {
	ctx := context.Background()
	if _, err = p.conn.Prepare(ctx, UpdateSourceSQL, UpdateSourceSQL); err != nil {
		return err
	}
	if _, err = p.conn.Exec(ctx, UpdateSourceSQL, pgText(p.SourceID), pgInt8(int64(m.EndLsn)), pgTz(m.CommitTime)); err != nil {
		return err
	}
	if _, err = p.conn.Exec(ctx, "commit"); err != nil {
		return err
	}
	committed <- m.CommitLsn
	return
}

func ScanLSNFromLog(path string) (lsn string, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if match := LSNLogRegex.FindStringSubmatch(scanner.Text()); len(match) > 1 {
			lsn = match[1]
		}
		f.SetReadDeadline(time.Now().Add(time.Second))
	}
	if lsn == "" {
		return "", fmt.Errorf("lsn not found in log: %w", scanner.Err())
	}
	return lsn, nil
}

var LSNLogRegex = regexp.MustCompile(`(?:consistent recovery state reached at|redo done at) ([0-9A-F]{2,8}\/[0-9A-F]{2,8})`)

func pgText(t string) pgtype.Text {
	return pgtype.Text{String: t, Status: pgtype.Present}
}

func pgInt8(i int64) pgtype.Int8 {
	return pgtype.Int8{Int: i, Status: pgtype.Present}
}

func pgTz(ts uint64) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: pgTime2Time(ts), Status: pgtype.Present}
}

func pgTime2Time(ts uint64) time.Time {
	micro := microsecFromUnixEpochToY2K + int64(ts)
	return time.Unix(micro/microInSecond, (micro%microInSecond)*nsInSecond)
}

const microInSecond = int64(1e6)
const nsInSecond = int64(1e3)
const microsecFromUnixEpochToY2K = int64(946684800 * 1000000)
