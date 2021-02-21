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
	pending  map[uint64]pulsar.MessageID
}

var ReceiverQueueSize = 1000

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
		Name:              host,
		Topic:             p.PulsarTopic,
		SubscriptionName:  p.PulsarSubscription,
		ReceiverQueueSize: ReceiverQueueSize,
		Type:              pulsar.KeyShared,
	})
	if err != nil {
		return nil, err
	}

	p.pending = make(map[uint64]pulsar.MessageID, ReceiverQueueSize)

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
		p.pending[uint64(lsn)] = msg.ID()
		p.mu.Unlock()

		change = Change{Checkpoint: Checkpoint{LSN: uint64(lsn)}, Message: m}
		return
	}, func() {
		p.consumer.Close()
		p.client.Close()
	})
}

func (p *PulsarConsumerSource) Commit(cp Checkpoint) {
	if id := p.unAckID(cp); id != nil {
		p.consumer.AckID(id)
	}
}

func (p *PulsarConsumerSource) Abort(cp Checkpoint) {
	if id := p.unAckID(cp); id != nil {
		p.consumer.NackID(id)
	}
}

func (p *PulsarConsumerSource) unAckID(cp Checkpoint) (id pulsar.MessageID) {
	var ok bool
	p.mu.Lock()
	if id, ok = p.pending[cp.LSN]; ok {
		delete(p.pending, cp.LSN)
	}
	p.mu.Unlock()
	return
}

func startMessageID(cp Checkpoint) (sid pulsar.MessageID) {
	if cp.Time.IsZero() {
		sid = pulsar.LatestMessageID()
	} else {
		sid = pulsar.EarliestMessageID()
	}
	return
}
