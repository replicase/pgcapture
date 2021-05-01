package source

import (
	"context"
	"encoding/hex"
	"os"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

var ReceiverQueueSize = 5000

type PulsarReaderSource struct {
	BaseSource

	PulsarOption pulsar.ClientOptions
	PulsarTopic  string

	client     pulsar.Client
	reader     pulsar.Reader
	consistent bool
	seekOffset time.Duration
	log        *logrus.Entry
}

func (p *PulsarReaderSource) Capture(cp Checkpoint) (changes chan Change, err error) {
	if p.seekOffset == 0 {
		p.seekOffset = -1 * time.Second
	}

	p.log = logrus.WithFields(logrus.Fields{"From": "PulsarReaderSource", "Topic": p.PulsarTopic})

	host, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	p.client, err = pulsar.NewClient(p.PulsarOption)
	if err != nil {
		return nil, err
	}

	start := pulsar.EarliestMessageID()
	seekTs := time.Time{}
	if id, err := pulsar.DeserializeMessageID(cp.Data); err == nil {
		start = id
	} else {
		if ts, err := time.Parse(time.RFC3339Nano, string(cp.Data)); err == nil {
			seekTs = ts.Add(p.seekOffset)
		}
	}

	p.reader, err = p.client.CreateReader(pulsar.ReaderOptions{
		Name:                    host,
		Topic:                   p.PulsarTopic,
		StartMessageID:          start,
		StartMessageIDInclusive: true,
		ReceiverQueueSize:       ReceiverQueueSize,
	})
	if err != nil {
		return nil, err
	}

	if !seekTs.IsZero() {
		if err = p.reader.SeekByTime(seekTs); err != nil {
			return nil, err
		}
		p.log.WithFields(logrus.Fields{
			"SeekTs":      seekTs,
			"RequiredLSN": cp.LSN,
		}).Info("start reading pulsar topic from requested timestamp")
	} else {
		p.log.WithFields(logrus.Fields{
			"RequiredLSN":    cp.LSN,
			"RequiredMIDHex": hex.EncodeToString(start.Serialize()),
		}).Info("start reading pulsar topic from requested position")
	}

	var first bool

	return p.BaseSource.capture(func(ctx context.Context) (change Change, err error) {
		var msg pulsar.Message

		msg, err = p.reader.Next(ctx)
		if err != nil {
			return
		}

		checkpoint, err := ToCheckpoint(msg)
		if err != nil {
			return
		}

		m := &pb.Message{}
		if err = proto.Unmarshal(msg.Payload(), m); err != nil {
			return
		}

		if !first {
			p.log.WithFields(logrus.Fields{
				"MessageLSN":  checkpoint.LSN,
				"RequiredLSN": cp.LSN,
				"Message":     m.String(),
			}).Info("retrieved the first message from pulsar")
			first = true
		}

		if !p.consistent && cp.LSN != 0 {
			p.consistent = checkpoint.Equal(cp) || checkpoint.After(cp)
			p.log.WithFields(logrus.Fields{
				"MessageLSN":  checkpoint.LSN,
				"RequiredLSN": cp.LSN,
				"Consistent":  p.consistent,
				"Message":     m.String(),
			}).Info("still catching lsn from pulsar")
			return
		}

		if !p.consistent && cp.LSN == 0 {
			p.consistent = m.GetBegin() != nil
			p.log.WithFields(logrus.Fields{
				"MessageLSN":  checkpoint.LSN,
				"RequiredLSN": cp.LSN,
				"Consistent":  p.consistent,
				"Message":     m.String(),
			}).Info("still waiting for the first begin message")
			if !p.consistent {
				return
			}
		}

		change = Change{Checkpoint: checkpoint, Message: m}
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
	log      *logrus.Entry
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
		Name:              host,
		Topic:             p.PulsarTopic,
		SubscriptionName:  p.PulsarSubscription,
		ReceiverQueueSize: ReceiverQueueSize,
		Type:              pulsar.Shared, // not use key_shared on xid, because transaction sizes are vary dramatically
	})
	if err != nil {
		return nil, err
	}

	p.log = logrus.WithFields(logrus.Fields{
		"From":         "PulsarConsumerSource",
		"Topic":        p.PulsarTopic,
		"Subscription": p.PulsarSubscription,
	})

	var first bool
	return p.BaseSource.capture(func(ctx context.Context) (change Change, err error) {
		var msg pulsar.Message

		msg, err = p.consumer.Receive(ctx)
		if err != nil {
			return
		}
		checkpoint, err := ToCheckpoint(msg)
		if err != nil {
			return
		}

		m := &pb.Message{}
		if err = proto.Unmarshal(msg.Payload(), m); err != nil {
			return
		}

		if !first {
			p.log.WithFields(logrus.Fields{
				"MessageLSN": checkpoint.LSN,
				"Message":    m.String(),
			}).Info("retrieved the first message from pulsar")
			first = true
		}

		if m.GetChange() == nil {
			p.consumer.Ack(msg)
			return
		}

		change = Change{Checkpoint: checkpoint, Message: m}
		return
	}, func() {
		p.consumer.Close()
		p.client.Close()
	})
}

func (p *PulsarConsumerSource) Commit(cp Checkpoint) {
	if mid, err := pulsar.DeserializeMessageID(cp.Data); err == nil {
		p.consumer.AckID(mid)
	}
}

func (p *PulsarConsumerSource) Requeue(cp Checkpoint, reason string) {
	if mid, err := pulsar.DeserializeMessageID(cp.Data); err == nil {
		p.consumer.NackID(mid)
	}
}
