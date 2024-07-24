package source

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/replicase/pgcapture/pkg/cursor"
	"github.com/replicase/pgcapture/pkg/decode"
	"github.com/replicase/pgcapture/pkg/pb"
	"github.com/replicase/pgcapture/pkg/sql"
	"github.com/sirupsen/logrus"
)

type PGXSource struct {
	BaseSource

	SetupConnStr      string
	ReplConnStr       string
	ReplSlot          string
	CreateSlot        bool
	CreatePublication bool
	StartLSN          string
	DecodePlugin      string

	setupConn      *pgx.Conn
	replConn       *pgconn.PgConn
	schema         *decode.PGXSchemaLoader
	decoder        decode.Decoder
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

func (p *PGXSource) Capture(cp cursor.Checkpoint) (changes chan Change, err error) {
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

	if _, err = p.setupConn.Exec(ctx, sql.InstallExtension); err != nil {
		return nil, err
	}

	p.schema = decode.NewPGXSchemaLoader(p.setupConn)
	if err = p.schema.RefreshType(); err != nil {
		return nil, err
	}

	switch p.DecodePlugin {
	case decode.PGLogicalOutputPlugin:
		p.decoder, err = decode.NewPGLogicalDecoder(p.schema)
		if err != nil {
			return nil, err
		}
	case decode.PGOutputPlugin:
		p.decoder = decode.NewPGOutputDecoder(p.schema, p.ReplSlot)
		if p.CreatePublication {
			if _, err = p.setupConn.Exec(ctx, fmt.Sprintf(sql.CreatePublication, p.ReplSlot)); err != nil {
				var pge *pgconn.PgError
				if !errors.As(err, &pge) || pge.Code != "42710" {
					return nil, err
				}
			}
		}
	default:
		return nil, errors.New("unknown decode plugin")
	}

	if p.CreateSlot {
		if _, err = p.setupConn.Exec(ctx, sql.CreateLogicalSlot, p.ReplSlot, p.DecodePlugin); err != nil {
			var pge *pgconn.PgError
			if !errors.As(err, &pge) || pge.Code != "42710" {
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

	var confirmedFlushLSN pglogrepl.LSN
	if err = p.setupConn.QueryRow(context.Background(), sql.QueryReplicationSlot, p.ReplSlot).Scan(&confirmedFlushLSN); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("replication slot not found")
		}
		return nil, err
	}

	p.log = logrus.WithFields(logrus.Fields{"From": "PGXSource"})
	p.log.WithFields(logrus.Fields{
		"SystemID":                  ident.SystemID,
		"Timeline":                  ident.Timeline,
		"XLogPos":                   int64(ident.XLogPos),
		"DBName":                    ident.DBName,
		"Decoder":                   p.DecodePlugin,
		"ReplSlot":                  p.ReplSlot,
		"ReplSlotConfirmedFlushLSN": uint64(confirmedFlushLSN),
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
			p.currentLsn = uint64(confirmedFlushLSN)
		}
		p.currentSeq = 0
		p.log.WithFields(logrus.Fields{
			"ReplSlot": p.ReplSlot,
			"FromLSN":  p.currentLsn,
		}).Info("start logical replication from the latest position")
	}
	p.Commit(cursor.Checkpoint{LSN: p.currentLsn})
	if err = pglogrepl.StartReplication(
		context.Background(),
		p.replConn,
		p.ReplSlot,
		pglogrepl.LSN(p.currentLsn),
		pglogrepl.StartReplicationOptions{PluginArgs: p.decoder.GetPluginArgs()},
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
			pkm, err = pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				return change, err
			}

			if pkm.ReplyRequested {
				p.nextReportTime = time.Time{}
			}
			change = Change{
				Checkpoint: cursor.Checkpoint{
					LSN:        uint64(pkm.ServerWALEnd),
					ServerTime: pkm.ServerTime,
				},
				Message: &pb.Message{
					Type: &pb.Message_KeepAlive{
						KeepAlive: &pb.KeepAlive{},
					},
				},
			}
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return change, err
			}
			// in the implementation of pgx v5, the xld.WALData will be reused
			walData := make([]byte, len(xld.WALData))
			copy(walData, xld.WALData)
			m, err := p.decoder.Decode(walData)
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
				p.currentLsn = c.CommitLsn
				p.currentSeq++
			}
			change = Change{
				Checkpoint: cursor.Checkpoint{LSN: p.currentLsn, Seq: p.currentSeq, ServerTime: xld.ServerTime},
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

func (p *PGXSource) Commit(cp cursor.Checkpoint) {
	if cp.LSN != 0 {
		atomic.StoreUint64(&p.ackLsn, cp.LSN)
		atomic.AddUint64(&p.txCounter, 1)
	}
}

func (p *PGXSource) Requeue(cp cursor.Checkpoint, reason string) {
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
