package sink

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/golang/protobuf/proto"
	"github.com/jackc/pglogrepl"
	"github.com/rueian/pgcapture/pkg/source"
	"os"
	"time"
)

type PulsarSink struct {
	BaseSink

	PulsarOption pulsar.ClientOptions
	PulsarTopic  string
	client       pulsar.Client
	producer     pulsar.Producer
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	msg, err := reader.Next(ctx)
	cancel()
	if msg != nil {
		cp.Time = msg.EventTime()
		l, err := pglogrepl.ParseLSN(msg.Properties()["lsn"])
		if err != nil {
			return cp, err
		}
		cp.LSN = uint64(l)
	}
	if err != nil && err != context.DeadlineExceeded {
		return cp, err
	}

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

	return cp, nil
}

func (p *PulsarSink) Apply(changes chan source.Change) chan source.Checkpoint {
	return p.BaseSink.apply(changes, func(change source.Change, committed chan source.Checkpoint) error {
		seq := int64(change.Checkpoint.LSN)

		bs, err := proto.Marshal(change.Message)
		if err != nil {
			return err
		}

		p.producer.SendAsync(context.Background(), &pulsar.ProducerMessage{
			Payload:    bs,
			Properties: map[string]string{"lsn": pglogrepl.LSN(change.Checkpoint.LSN).String()},
			EventTime:  change.Checkpoint.Time,
			SequenceID: &seq,
		}, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
			if err != nil {
				p.BaseSink.err = err
				p.BaseSink.Stop()
				return
			}
			committed <- change.Checkpoint
		})
		return nil
	}, func() {
		p.producer.Flush()
		p.producer.Close()
		p.client.Close()
	})
}
