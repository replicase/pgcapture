package sink

import (
	"context"
	"encoding/hex"
	"os"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/jackc/pglogrepl"
	"github.com/rueian/pgcapture/pkg/source"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type PulsarSink struct {
	BaseSink

	PulsarOption pulsar.ClientOptions
	PulsarTopic  string

	client   pulsar.Client
	producer pulsar.Producer
	log      *logrus.Entry
	lsn      uint64
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
			l, err := pglogrepl.ParseLSN(msg.Key())
			if err != nil {
				return cp, err
			}
			cp.LSN = uint64(l)
			p.lsn = cp.LSN
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
		"SinkLastLSN": p.lsn,
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
				"SinkLastLSN": p.lsn,
			}).Info("applying the first message from source")
			first = true
		}
		if p.lsn >= change.Checkpoint.LSN {
			p.log.WithFields(logrus.Fields{
				"MessageLSN":  change.Checkpoint.LSN,
				"SinkLastLSN": p.lsn,
			}).Warn("message dropped due to its lsn smaller than the last lsn of sink")
			return nil
		}

		// correct the lsn check for the next begin message check
		if commit := change.Message.GetCommit(); commit != nil {
			p.lsn = commit.EndLsn
		} else if change.Message.GetBegin() == nil {
			// do not update the lsn check on begin message
			p.lsn = change.Checkpoint.LSN
		}

		seq := int64(change.Checkpoint.LSN)
		lsn := pglogrepl.LSN(change.Checkpoint.LSN).String()

		bs, err := proto.Marshal(change.Message)
		if err != nil {
			return err
		}

		p.producer.SendAsync(context.Background(), &pulsar.ProducerMessage{
			Key:        lsn, // for topic compaction, not routing policy
			Payload:    bs,
			SequenceID: &seq,
		}, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
			if err != nil {
				p.log.WithFields(logrus.Fields{
					"MessageLSN":   change.Checkpoint.LSN,
					"MessageIDHex": hex.EncodeToString(id.Serialize()),
				}).Errorf("fail to send message to pulsar: %v", err)
				p.BaseSink.err.Store(err)
				p.BaseSink.Stop()
				return
			}
			committed <- change.Checkpoint
		})
		return nil
	})
}
