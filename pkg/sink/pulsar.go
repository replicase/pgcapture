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

	commitTime time.Time
}

func (p *PulsarSink) Setup() (lsn uint64, err error) {
	p.client, err = pulsar.NewClient(p.PulsarOption)
	if err != nil {
		return 0, err
	}

	host, err := os.Hostname()
	if err != nil {
		return 0, err
	}

	reader, err := p.client.CreateReader(pulsar.ReaderOptions{
		Topic:                   p.PulsarTopic,
		Name:                    p.PulsarTopic + "-producer",
		StartMessageID:          pulsar.LatestMessageID(),
		StartMessageIDInclusive: true,
	})
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	var str string
	for reader.HasNext() {
		msg, err := reader.Next(context.Background())
		if err != nil {
			return 0, err
		}
		str = msg.Properties()["lsn"]
	}

	var l pglogrepl.LSN
	if str != "" {
		l, err = pglogrepl.ParseLSN(str)
		if err != nil {
			return 0, err
		}
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
		return 0, err
	}

	return uint64(l), nil
}

func (p *PulsarSink) Apply(changes chan source.Change) chan uint64 {
	return p.BaseSink.apply(changes, func(change source.Change, committed chan uint64) error {
		seq := int64(change.LSN)
		lsn := pglogrepl.LSN(change.LSN)

		if begin := change.Message.GetBegin(); begin != nil {
			p.commitTime = pgTime2Time(begin.CommitTime)
		}

		bs, err := proto.Marshal(change.Message)
		if err != nil {
			return err
		}

		p.producer.SendAsync(context.Background(), &pulsar.ProducerMessage{
			Payload:    bs,
			Properties: map[string]string{"lsn": lsn.String()},
			EventTime:  p.commitTime,
			SequenceID: &seq,
		}, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
			if err != nil {
				p.BaseSink.err = err
				p.BaseSink.Stop()
				return
			}
			committed <- change.LSN
		})
		return nil
	}, func() {
		p.producer.Flush()
		p.producer.Close()
		p.client.Close()
	})
}
