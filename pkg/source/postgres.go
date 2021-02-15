package source

import (
	"context"
	"errors"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
	"github.com/rueian/pgcapture/pkg/decode"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/sql"
	"log"
	"sync/atomic"
	"time"
)

type PGXSource struct {
	SetupConnStr string
	ReplConnStr  string
	ReplSlot     string
	CreateSlot   bool

	setupConn *pgx.Conn
	replConn  *pgconn.PgConn

	schema  *decode.PGXSchemaLoader
	decoder *decode.PGLogicalDecoder

	ackLsn  uint64
	stopped int64
	stop    chan struct{}
}

func (p *PGXSource) Setup() (err error) {
	ctx := context.Background()
	p.setupConn, err = pgx.Connect(ctx, p.SetupConnStr)
	if err != nil {
		return err
	}
	p.schema = decode.NewPGXSchemaLoader(p.setupConn)
	if err = p.schema.RefreshType(); err != nil {
		return err
	}

	p.decoder = decode.NewPGLogicalDecoder(p.schema)

	if _, err = p.setupConn.Exec(ctx, sql.InstallExtension); err != nil {
		return nil
	}

	if p.CreateSlot {
		_, err = p.setupConn.Exec(ctx, sql.CreateLogicalSlot, p.ReplSlot, OutputPlugin)
	}

	return err
}

func (p *PGXSource) Capture(lsn uint64) (changes chan *pb.Message, err error) {
	p.replConn, err = pgconn.Connect(context.Background(), p.ReplConnStr)
	if err != nil {
		return nil, err
	}

	ident, err := pglogrepl.IdentifySystem(context.Background(), p.replConn)
	if err != nil {
		return nil, err
	}
	log.Println("SystemID:", ident.SystemID, "Timeline:", ident.Timeline, "XLogPos:", ident.XLogPos, "DBName:", ident.DBName)

	var requestLSN pglogrepl.LSN
	if lsn != 0 {
		requestLSN = pglogrepl.LSN(lsn)
		log.Println("start logical replication on slot with requested position", p.ReplSlot, requestLSN)
	} else {
		requestLSN = ident.XLogPos
		log.Println("start logical replication on slot with previous position", p.ReplSlot, requestLSN)
	}
	if err = pglogrepl.StartReplication(context.Background(), p.replConn, p.ReplSlot, requestLSN, pglogrepl.StartReplicationOptions{PluginArgs: pgLogicalParam}); err != nil {
		return nil, err
	}
	p.ackLsn = uint64(requestLSN)
	p.stop = make(chan struct{})

	changes = make(chan *pb.Message, 100)
	go func() {
		defer p.cleanup()
		defer close(p.stop)
		defer close(changes)
		if err = p.fetching(changes); err != nil {
			log.Fatalf("Logical replication failed: %v", err)
		}
	}()

	return changes, nil
}

func (p *PGXSource) fetching(changes chan *pb.Message) (err error) {
	reportInterval := time.Second * 5
	nextReportTime := time.Now().Add(reportInterval)

	for {
		if time.Now().After(nextReportTime) {
			if err = pglogrepl.SendStandbyStatusUpdate(context.Background(), p.replConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: p.committedLSN()}); err != nil {
				return err
			}
			nextReportTime = time.Now().Add(reportInterval)
			if atomic.LoadInt64(&p.stopped) == 1 {
				return nil
			}
		}

		ctx, cancel := context.WithDeadline(context.Background(), nextReportTime)
		msg, err := p.replConn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return err
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return err
				}
				if pkm.ReplyRequested {
					nextReportTime = time.Time{}
				}
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return err
				}
				m, err := p.decoder.Decode(xld.WALData)
				if err != nil {
					return err
				}
				if m != nil {
					if change := m.GetChange(); change != nil {
						if decode.Ignore(change) {
							continue
						} else if decode.IsDDL(change) {
							if err = p.schema.RefreshType(); err != nil {
								return err
							}
						}
					}
					changes <- m
				}
			}
		default:
			return errors.New("unexpected message")
		}
	}
}

func (p *PGXSource) Commit(lsn uint64) {
	atomic.StoreUint64(&p.ackLsn, lsn)
}

func (p *PGXSource) committedLSN() (lsn pglogrepl.LSN) {
	return pglogrepl.LSN(atomic.LoadUint64(&p.ackLsn))
}

func (p *PGXSource) Stop() {
	atomic.StoreInt64(&p.stopped, 1)
	if p.stop != nil {
		<-p.stop
	}
}

func (p *PGXSource) cleanup() {
	if p.setupConn != nil {
		p.setupConn.Close(context.Background())
		p.setupConn = nil
	}
	if p.replConn != nil {
		p.replConn.Close(context.Background())
		p.replConn = nil
	}
}

const OutputPlugin = "pglogical_output"

var pgLogicalParam = []string{
	"min_proto_version '1'",
	"max_proto_version '1'",
	"startup_params_format '1'",
	"\"binary.want_binary_basetypes\" '1'",
	"\"binary.basetypes_major_version\" '906'",
	"\"binary.bigendian\" '1'",
}
