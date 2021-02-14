package sink

import (
	"bufio"
	"context"
	"fmt"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/sql"
	"os"
	"regexp"
	"sync/atomic"
	"time"
)

type PGXSink struct {
	ConnStr  string
	LogPath  string
	SourceID string

	conn *pgx.Conn
	raw  *pgconn.PgConn
	err  error

	stopped uint64
}

func (p *PGXSink) Setup() (lsn uint64, err error) {
	ctx := context.Background()
	p.conn, err = pgx.Connect(ctx, p.ConnStr)
	if err != nil {
		return 0, err
	}

	if _, err = p.conn.Exec(ctx, sql.InstallExtension); err != nil {
		return 0, err
	}

	var str string
	err = p.conn.QueryRow(ctx, "SELECT commit FROM pgcapture.sources WHERE id = $1 AND message IS NULL", p.SourceID).Scan(&str)
	// try to load from log file
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

	p.raw = p.conn.PgConn()
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
				case *pb.Message_Change:
					p.err = p.handleChange(change.GetChange())
				case *pb.Message_Commit:
					p.err = p.handleCommit(change.GetCommit(), committed)
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
	if m.Namespace == "pgcapture" && m.Table == "ddl_logs" {
		return p.handleDDL(ctx, m)
	} else {

	}
	return err
}

func (p *PGXSink) handleDDL(ctx context.Context, m *pb.Change) (err error) {
	for _, field := range m.NewTuple {
		if field.Name == "query" {
			_, err = p.conn.Exec(ctx, string(field.Datum))
			return err
		}
	}
	return nil
}

const (
	UpdateSourceSQL = "insert into pgcapture.sources(id,commit,commit_ts) values ($1,$2,$3) on conflict (id) do update set commit=EXCLUDED.commit,commit_ts=EXCLUDED.commit_ts,apply_ts=now()"
)

func (p *PGXSink) handleCommit(m *pb.Commit, committed chan uint64) (err error) {
	ctx := context.Background()
	if _, err = p.conn.Prepare(ctx, UpdateSourceSQL, UpdateSourceSQL); err != nil {
		return err
	}
	if _, err = p.conn.Exec(ctx, UpdateSourceSQL, pgText(p.SourceID), pgInt8(int64(m.CommitLsn)), pgTz(m.CommitTime)); err != nil {
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
