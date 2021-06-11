package sink

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/rueian/pgcapture/pkg/source"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type PulsarSink struct {
	BaseSink

	PulsarOption pulsar.ClientOptions
	PulsarTopic  string

	client     pulsar.Client
	producer   pulsar.Producer
	log        *logrus.Entry
	prev       source.Checkpoint
	consistent bool
}

func (p *PulsarSink) Setup() (cp source.Checkpoint, err error) {
	p.client, err = pulsar.NewClient(p.PulsarOption)
	if err != nil {
		return cp, err
	}

	host, err := os.Hostname()
	if err != nil {
		return cp, err
	}

	reader, err := p.client.CreateReader(pulsar.ReaderOptions{
		Topic:                   p.PulsarTopic,
		Name:                    p.PulsarTopic + "-producer",
		StartMessageID:          pulsar.LatestMessageID(),
		StartMessageIDInclusive: true,
	})
	if err != nil {
		return cp, err
	}
	defer reader.Close()

	for reader.HasNext() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		msg, err := reader.Next(ctx)
		cancel()
		if msg != nil {
			cp, err = source.ToCheckpoint(msg)
			if err != nil {
				return cp, err
			}
			p.prev = cp
		}
		if err != nil && err != context.DeadlineExceeded {
			return cp, err
		}
	}

	p.log = logrus.WithFields(logrus.Fields{
		"From":  "PulsarSink",
		"Topic": p.PulsarTopic,
	})
	p.log.WithFields(logrus.Fields{
		"SinkLastLSN": p.prev.LSN,
	}).Info("start sending changes to pulsar")

	p.producer, err = p.client.CreateProducer(pulsar.ProducerOptions{
		Topic:               p.PulsarTopic,
		Name:                p.PulsarTopic + "-producer", // fixed for exclusive producer
		Properties:          map[string]string{"host": host},
		MaxPendingMessages:  2000,
		CompressionType:     pulsar.ZSTD,
		BatchingMaxMessages: 1000,
		BatchingMaxSize:     1024 * 1024,
	})
	if err != nil {
		return cp, err
	}

	p.BaseSink.CleanFn = func() {
		p.producer.Flush()
		p.producer.Close()
		p.client.Close()
	}

	return cp, nil
}

func (p *PulsarSink) Apply(changes chan source.Change) chan source.Checkpoint {
	var first bool
	return p.BaseSink.apply(changes, func(change source.Change, committed chan source.Checkpoint) error {
		if !first {
			p.log.WithFields(logrus.Fields{
				"MessageLSN":  change.Checkpoint.LSN,
				"SinkLastLSN": p.prev.LSN,
				"Message":     change.Message.String(),
			}).Info("applying the first message from source")
			first = true
		}

		p.consistent = p.consistent || change.Checkpoint.After(p.prev)
		if !p.consistent {
			p.log.WithFields(logrus.Fields{
				"MessageLSN":  change.Checkpoint.LSN,
				"SinkLastLSN": p.prev.LSN,
				"Message":     change.Message.String(),
			}).Warn("message dropped due to its lsn smaller than the last lsn of sink")
			return nil
		}

		bs, err := proto.Marshal(change.Message)
		if err != nil {
			return err
		}

		p.producer.SendAsync(context.Background(), &pulsar.ProducerMessage{
			Key:     change.Checkpoint.ToKey(), // for topic compaction, not routing policy
			Payload: bs,
		}, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
			if err != nil {
				var idHex string
				if id != nil {
					idHex = hex.EncodeToString(id.Serialize())
				}
				p.log.WithFields(logrus.Fields{
					"MessageLSN":   change.Checkpoint.LSN,
					"MessageIDHex": idHex,
				}).Errorf("fail to send message to pulsar: %v", err)
				p.BaseSink.err.Store(fmt.Errorf("%w", err))
				p.BaseSink.Stop()
				return
			}
			committed <- change.Checkpoint
		})
		return nil
	})
}
