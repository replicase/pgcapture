package source

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/golang/protobuf/proto"
	"github.com/jackc/pglogrepl"
	"github.com/rueian/pgcapture/pkg/pb"
	"log"
	"os"
	"sync/atomic"
	"time"
)

type PulsarSource struct {
	PulsarOption pulsar.ClientOptions
	PulsarTopic  string
	client       pulsar.Client
	reader       pulsar.Reader

	stopped int64
	stop    chan struct{}
}

func (p *PulsarSource) Setup() (err error) {
	p.client, err = pulsar.NewClient(p.PulsarOption)
	return err
}

func (p *PulsarSource) Capture(cp Checkpoint) (changes chan Change, err error) {
	var mid pulsar.MessageID

	if cp.LSN == 0 {
		mid = pulsar.LatestMessageID()
	} else if cp.MID != nil {
		if mid, err = pulsar.DeserializeMessageID(cp.MID); err != nil {
			return nil, err
		}
	}

	host, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	p.reader, err = p.client.CreateReader(pulsar.ReaderOptions{
		Name:                    host,
		Topic:                   p.PulsarTopic,
		StartMessageID:          mid,
		StartMessageIDInclusive: true,
	})
	if err != nil {
		return nil, err
	}

	if cp.MID == nil && !cp.Time.IsZero() {
		if err = p.reader.SeekByTime(cp.Time.Add(-1 * time.Second)); err != nil {
			return nil, err
		}
	}

	changes = make(chan Change, 100)
	p.stop = make(chan struct{})
	go func(cp Checkpoint) {
		defer close(changes)
		defer close(p.stop)
		var consistent bool
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			msg, err := p.reader.Next(ctx)
			cancel()
			if err == context.DeadlineExceeded {
				if atomic.LoadInt64(&p.stopped) == 1 {
					return
				}
				continue
			}
			if err != nil {
				log.Fatalf("from pulsar failed: %v", err)
				return
			}
			lsn, err := pglogrepl.ParseLSN(msg.Properties()["lsn"])
			if err != nil {
				log.Fatalf("from pulsar parse lsn failed: %v", err)
				return
			}

			if !consistent && cp.LSN != 0 {
				consistent = cp.LSN == uint64(lsn)
				continue
			}

			m := &pb.Message{}
			if err = proto.Unmarshal(msg.Payload(), m); err != nil {
				log.Fatalf("from pulsar parse proto failed: %v", err)
				return
			}

			if !consistent && cp.LSN == 0 {
				if consistent = m.GetBegin() != nil; !consistent {
					continue
				}
			}

			changes <- Change{
				Checkpoint: Checkpoint{
					LSN:  uint64(lsn),
					MID:  msg.ID().Serialize(),
					Time: msg.EventTime(),
				},
				Message: m,
			}
		}
	}(cp)
	return changes, nil
}

func (p *PulsarSource) Commit(cp Checkpoint) {
	return
}

func (p *PulsarSource) Stop() {
	atomic.StoreInt64(&p.stopped, 1)
	if p.stop != nil {
		<-p.stop
	}
	p.reader.Close()
	p.client.Close()
}
