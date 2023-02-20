package sink

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/rueian/pgcapture/pkg/cursor"
	"github.com/rueian/pgcapture/pkg/source"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type PulsarSink struct {
	BaseSink

	PulsarOption pulsar.ClientOptions
	PulsarTopic  string
	// For overriding the cluster list to be replicated to
	ReplicatedClusters []string

	client     pulsar.Client
	tracker    cursor.Tracker
	producer   pulsar.Producer
	log        *logrus.Entry
	prev       cursor.Checkpoint
	consistent bool
}

func (p *PulsarSink) Setup() (cp cursor.Checkpoint, err error) {
	p.client, err = pulsar.NewClient(p.PulsarOption)
	if err != nil {
		return cp, err
	}

	host, err := os.Hostname()
	if err != nil {
		return cp, err
	}

	p.tracker = &cursor.PulsarTracker{
		Client:      p.client,
		PulsarTopic: p.PulsarTopic,
	}

	cp, err = p.tracker.Last()
	if err != nil {
		return cp, err
	}
	p.prev = cp

	p.log = logrus.WithFields(logrus.Fields{
		"From":  "PulsarSink",
		"Topic": p.PulsarTopic,
	})
	p.log.WithFields(logrus.Fields{
		"SinkLastLSN": p.prev.LSN,
		"SinkLastSeq": p.prev.Seq,
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

func (p *PulsarSink) Apply(changes chan source.Change) chan cursor.Checkpoint {
	var first bool
	return p.BaseSink.apply(changes, func(change source.Change, committed chan cursor.Checkpoint) error {
		if !first {
			p.log.WithFields(logrus.Fields{
				"MessageLSN":         change.Checkpoint.LSN,
				"MessageSeq":         change.Checkpoint.Seq,
				"SinkLastLSN":        p.prev.LSN,
				"SinkLastSeq":        p.prev.Seq,
				"Message":            change.Message.String(),
				"ReplicatedClusters": p.ReplicatedClusters,
			}).Info("applying the first message from source")
			first = true
		}

		p.consistent = p.consistent || change.Checkpoint.After(p.prev)
		if !p.consistent {
			p.log.WithFields(logrus.Fields{
				"MessageLSN":         change.Checkpoint.LSN,
				"MessageSeq":         change.Checkpoint.Seq,
				"SinkLastLSN":        p.prev.LSN,
				"SinkLastSeq":        p.prev.Seq,
				"Message":            change.Message.String(),
				"ReplicatedClusters": p.ReplicatedClusters,
			}).Warn("message dropped due to its lsn smaller than the last lsn of sink")
			return nil
		}

		bs, err := proto.Marshal(change.Message)
		if err != nil {
			return err
		}

		p.producer.SendAsync(context.Background(), &pulsar.ProducerMessage{
			Key:                 change.Checkpoint.ToKey(), // for topic compaction, not routing policy
			Payload:             bs,
			ReplicationClusters: p.ReplicatedClusters,
		}, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
			if err != nil {
				var idHex string
				if id != nil {
					idHex = hex.EncodeToString(id.Serialize())
				}
				p.log.WithFields(logrus.Fields{
					"MessageLSN":         change.Checkpoint.LSN,
					"MessageIDHex":       idHex,
					"ReplicatedClusters": p.ReplicatedClusters,
				}).Errorf("fail to send message to pulsar: %v", err)
				p.BaseSink.err.Store(fmt.Errorf("%w", err))
				p.BaseSink.Stop()
				return
			}

			cp := change.Checkpoint
			if err := p.tracker.Commit(cp); err != nil {
				p.log.WithFields(logrus.Fields{
					"MessageLSN": change.Checkpoint.LSN,
					"MessageSeq": change.Checkpoint.Seq,
				}).Errorf("fail to commit message to tracker: %v", err)
			}
			committed <- cp
		})
		return nil
	})
}
