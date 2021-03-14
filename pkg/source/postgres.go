package source

import (
	"context"
	"errors"
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

	setupConn      *pgx.Conn
	replConn       *pgconn.PgConn
	schema         *decode.PGXSchemaLoader
	decoder        *decode.PGLogicalDecoder
	nextReportTime time.Time
	ackLsn         uint64
	log            *logrus.Entry
	first          bool
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
		"XLogPos":  ident.XLogPos,
		"DBName":   ident.DBName,
	}).Info("retrieved current info of source database")

	var requestLSN pglogrepl.LSN
	if cp.LSN != 0 {
		requestLSN = pglogrepl.LSN(cp.LSN)
		p.log.WithFields(logrus.Fields{
			"ReplSlot": p.ReplSlot,
			"FromLSN":  requestLSN,
		}).Info("start logical replication from requested position")
	} else {
		requestLSN = ident.XLogPos
		p.log.WithFields(logrus.Fields{
			"ReplSlot": p.ReplSlot,
			"FromLSN":  requestLSN,
		}).Info("start logical replication from the latest position")
	}
	p.Commit(Checkpoint{LSN: uint64(requestLSN)})
	if err = pglogrepl.StartReplication(context.Background(), p.replConn, p.ReplSlot, requestLSN, pglogrepl.StartReplicationOptions{PluginArgs: decode.PGLogicalParam}); err != nil {
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
			}
			change = Change{
				// use WALStart as checkpoint instead of WALStart+len(WALData),
				// because WALStart is the only value guaranteed will only increase, not decrease.
				// However WALStart will be duplicated on tx boundary
				Checkpoint: Checkpoint{LSN: uint64(xld.WALStart)},
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
	}
}

func (p *PGXSource) Requeue(cp Checkpoint) {

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
