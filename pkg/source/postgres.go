package source

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
	"github.com/rueian/pgcapture/pkg/decode"
	"github.com/rueian/pgcapture/pkg/sql"
	"github.com/sirupsen/logrus"
)

type PGXSource struct {
	BaseSource

	SetupConnStr string
	ReplConnStr  string
	ReplSlot     string
	CreateSlot   bool
	StartLSN     string

	setupConn      *pgx.Conn
	replConn       *pgconn.PgConn
	schema         *decode.PGXSchemaLoader
	decoder        *decode.PGLogicalDecoder
	nextReportTime time.Time
	ackLsn         uint64
	txCounter      uint64
	log            *logrus.Entry
	first          bool
	currentLsn     uint64
	currentSeq     uint32
}

func (p *PGXSource) TxCounter() uint64 {
	return atomic.LoadUint64(&p.txCounter)
}

func (p *PGXSource) Capture(cp Checkpoint) (changes chan Change, err error) {
	defer func() {
		if err != nil {
			p.cleanup()
		}
	}()

	ctx := context.Background()
	p.setupConn, err = pgx.Connect(ctx, p.SetupConnStr)
	if err != nil {
		return nil, err
	}

	var sv string
	if err = p.setupConn.QueryRow(ctx, sql.ServerVersionNum).Scan(&sv); err != nil {
		return nil, err
	}

	svn, err := strconv.ParseInt(sv, 10, 64)
	if err != nil {
		return nil, err
	}

	if _, err = p.setupConn.Exec(ctx, sql.InstallExtension); err != nil {
		return nil, err
	}

	p.schema = decode.NewPGXSchemaLoader(p.setupConn)
	if err = p.schema.RefreshType(); err != nil {
		return nil, err
	}

	p.decoder = decode.NewPGLogicalDecoder(p.schema)

	if p.CreateSlot {
		if _, err = p.setupConn.Exec(ctx, sql.CreateLogicalSlot, p.ReplSlot, decode.OutputPlugin); err != nil {
			if pge, ok := err.(*pgconn.PgError); !ok || pge.Code != "42710" {
				return nil, err
			}
		}
	}

	p.replConn, err = pgconn.Connect(context.Background(), p.ReplConnStr)
	if err != nil {
		return nil, err
	}

	ident, err := pglogrepl.IdentifySystem(context.Background(), p.replConn)
	if err != nil {
		return nil, err
	}

	p.log = logrus.WithFields(logrus.Fields{"From": "PGXSource"})
	p.log.WithFields(logrus.Fields{
		"SystemID": ident.SystemID,
		"Timeline": ident.Timeline,
		"XLogPos":  int64(ident.XLogPos),
		"DBName":   ident.DBName,
	}).Info("retrieved current info of source database")

	if cp.LSN != 0 {
		p.currentLsn = cp.LSN
		p.currentSeq = cp.Seq
		p.log.WithFields(logrus.Fields{
			"ReplSlot": p.ReplSlot,
			"FromLSN":  p.currentLsn,
		}).Info("start logical replication from requested position")
	} else {
		if p.StartLSN != "" {
			startLsn, err := pglogrepl.ParseLSN(p.StartLSN)
			if err != nil {
				return nil, err
			}
			p.currentLsn = uint64(startLsn)
		} else {
			p.currentLsn = uint64(ident.XLogPos)
		}
		p.currentSeq = 0
		p.log.WithFields(logrus.Fields{
			"ReplSlot": p.ReplSlot,
			"FromLSN":  p.currentLsn,
		}).Info("start logical replication from the latest position")
	}
	p.Commit(Checkpoint{LSN: p.currentLsn})
	if err = pglogrepl.StartReplication(
		context.Background(),
		p.replConn,
		p.ReplSlot,
		pglogrepl.LSN(p.currentLsn),
		pglogrepl.StartReplicationOptions{PluginArgs: decode.PGLogicalParam(svn)},
	); err != nil {
		return nil, err
	}

	return p.BaseSource.capture(p.fetching, p.cleanup)
}

func (p *PGXSource) fetching(ctx context.Context) (change Change, err error) {
	if time.Now().After(p.nextReportTime) {
		if err = p.reportLSN(ctx); err != nil {
			return change, err
		}
		p.nextReportTime = time.Now().Add(5 * time.Second)
	}
	msg, err := p.replConn.ReceiveMessage(ctx)
	if err != nil {
		return change, err
	}
	switch msg := msg.(type) {
	case *pgproto3.CopyData:
		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			var pkm pglogrepl.PrimaryKeepaliveMessage
			if pkm, err = pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:]); err == nil && pkm.ReplyRequested {
				p.nextReportTime = time.Time{}
			}
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return change, err
			}
			m, err := p.decoder.Decode(xld.WALData)
			if m == nil || err != nil {
				return change, err
			}
			if msg := m.GetChange(); msg != nil {
				if decode.Ignore(msg) {
					return change, nil
				} else if decode.IsDDL(msg) {
					if err = p.schema.RefreshType(); err != nil {
						return change, err
					}
				}
				p.currentSeq++
			} else if b := m.GetBegin(); b != nil {
				p.currentLsn = b.FinalLsn
				p.currentSeq = 0
			} else if c := m.GetCommit(); c != nil {
				p.currentLsn = c.EndLsn
				p.currentSeq = 0
			}
			change = Change{
				Checkpoint: Checkpoint{LSN: p.currentLsn, Seq: p.currentSeq},
				Message:    m,
			}
			if !p.first {
				p.log.WithFields(logrus.Fields{
					"MessageLSN": change.Checkpoint.LSN,
					"Message":    m.String(),
				}).Info("retrieved the first message from postgres")
				p.first = true
			}
		}
	default:
		err = errors.New("unexpected message")
	}
	return change, err
}

func (p *PGXSource) Commit(cp Checkpoint) {
	if cp.LSN != 0 {
		atomic.StoreUint64(&p.ackLsn, cp.LSN)
		atomic.AddUint64(&p.txCounter, 1)
	}
}

func (p *PGXSource) Requeue(cp Checkpoint, reason string) {
}

func (p *PGXSource) committedLSN() (lsn pglogrepl.LSN) {
	return pglogrepl.LSN(atomic.LoadUint64(&p.ackLsn))
}

func (p *PGXSource) reportLSN(ctx context.Context) error {
	if committed := p.committedLSN(); committed != 0 {
		return pglogrepl.SendStandbyStatusUpdate(ctx, p.replConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: committed})
	}
	return nil
}

func (p *PGXSource) cleanup() {
	ctx := context.Background()
	if p.setupConn != nil {
		p.setupConn.Close(ctx)
	}
	if p.replConn != nil {
		p.reportLSN(ctx)
		p.replConn.Close(ctx)
	}
}
