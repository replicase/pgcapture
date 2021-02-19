package source

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/golang/protobuf/proto"
	"github.com/jackc/pglogrepl"
	"github.com/rueian/pgcapture/pkg/pb"
)

type PulsarReaderSource struct {
	BaseSource

	PulsarOption pulsar.ClientOptions
	PulsarTopic  string
	client       pulsar.Client
	reader       pulsar.Reader

	consistent bool
}

func (p *PulsarReaderSource) Capture(cp Checkpoint) (changes chan Change, err error) {
	host, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	p.client, err = pulsar.NewClient(p.PulsarOption)
	if err != nil {
		return nil, err
	}

	p.reader, err = p.client.CreateReader(pulsar.ReaderOptions{
		Name:                    host,
		Topic:                   p.PulsarTopic,
		StartMessageID:          startMessageID(cp),
		StartMessageIDInclusive: true,
	})
	if err != nil {
		return nil, err
	}

	if !cp.Time.IsZero() {
		ts := cp.Time.Add(-1 * time.Second)
		if err = p.reader.SeekByTime(ts); err != nil {
			return nil, err
		}
		log.Printf("seek pulsar topic %s from time: %v", p.PulsarTopic, ts)
	}

	return p.BaseSource.capture(func(ctx context.Context) (change Change, err error) {
		var msg pulsar.Message
		var lsn pglogrepl.LSN

		msg, err = p.reader.Next(ctx)
		if err != nil {
			return
		}
		lsn, err = pglogrepl.ParseLSN(msg.Properties()["lsn"])
		if err != nil {
			return
		}

		if !p.consistent && cp.LSN != 0 {
			p.consistent = cp.LSN == uint64(lsn)
			log.Printf("catching lsn from %s to %s", lsn, pglogrepl.LSN(cp.LSN))
			return
		}

		m := &pb.Message{}
		if err = proto.Unmarshal(msg.Payload(), m); err != nil {
			return
		}

		if !p.consistent && cp.LSN == 0 {
			if p.consistent = m.GetBegin() != nil; !p.consistent {
				return
			}
		}

		change = Change{Checkpoint: Checkpoint{LSN: uint64(lsn)}, Message: m}
		return
	}, func() {
		p.reader.Close()
		p.client.Close()
	})
}

func (p *PulsarReaderSource) Commit(cp Checkpoint) {
	return
}

type PulsarConsumerSource struct {
	BaseSource

	PulsarOption       pulsar.ClientOptions
	PulsarTopic        string
	PulsarSubscription string

	client   pulsar.Client
	consumer pulsar.Consumer
	mu       sync.Mutex
	unAcks   map[string][]pulsar.MessageID
	xidMap   map[uint64]string
}

func (p *PulsarConsumerSource) Capture(cp Checkpoint) (changes chan Change, err error) {
	host, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	p.client, err = pulsar.NewClient(p.PulsarOption)
	if err != nil {
		return nil, err
	}

	p.consumer, err = p.client.Subscribe(pulsar.ConsumerOptions{
		Name:             host,
		Topic:            p.PulsarTopic,
		SubscriptionName: p.PulsarSubscription,
		Type:             pulsar.KeyShared,
	})
	if err != nil {
		return nil, err
	}

	p.unAcks = make(map[string][]pulsar.MessageID, 100)
	p.xidMap = make(map[uint64]string, 100)

	return p.BaseSource.capture(func(ctx context.Context) (change Change, err error) {
		var msg pulsar.Message
		var lsn pglogrepl.LSN

		msg, err = p.consumer.Receive(ctx)
		if err != nil {
			return
		}
		lsn, err = pglogrepl.ParseLSN(msg.Properties()["lsn"])
		if err != nil {
			return
		}

		m := &pb.Message{}
		if err = proto.Unmarshal(msg.Payload(), m); err != nil {
			return
		}

		p.mu.Lock()
		xid := msg.Key()
		p.unAcks[xid] = append(p.unAcks[xid], msg.ID())
		if commit := m.GetCommit(); commit != nil {
			p.xidMap[uint64(lsn)] = xid
		}
		p.mu.Unlock()

		change = Change{Checkpoint: Checkpoint{LSN: uint64(lsn)}, Message: m}
		return
	}, func() {
		p.consumer.Close()
		p.client.Close()
	})
}

func (p *PulsarConsumerSource) Commit(cp Checkpoint) {
	p.txCommit(cp)
}

func (p *PulsarConsumerSource) txCommit(cp Checkpoint) {
	var ok bool
	var ids []pulsar.MessageID
	p.mu.Lock()
	xid := p.xidMap[cp.LSN]
	if ids, ok = p.unAcks[xid]; ok {
		delete(p.xidMap, cp.LSN)
		delete(p.unAcks, xid)
	}
	p.mu.Unlock()
	for _, id := range ids {
		p.consumer.AckID(id)
	}
}

func startMessageID(cp Checkpoint) (sid pulsar.MessageID) {
	if cp.Time.IsZero() {
		sid = pulsar.LatestMessageID()
	} else {
		sid = pulsar.EarliestMessageID()
	}
	return
}
