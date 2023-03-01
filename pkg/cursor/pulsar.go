package cursor

import (
	"context"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

var _ Tracker = (*PulsarTracker)(nil)

type PulsarTracker struct {
	Client      pulsar.Client
	PulsarTopic string
}

func (p *PulsarTracker) Last() (last Checkpoint, err error) {
	reader, err := p.Client.CreateReader(pulsar.ReaderOptions{
		Topic:                   p.PulsarTopic,
		Name:                    p.PulsarTopic + "-producer",
		StartMessageID:          pulsar.LatestMessageID(),
		StartMessageIDInclusive: true,
	})
	if err != nil {
		return last, err
	}
	defer reader.Close()

	var (
		msg pulsar.Message
	)
	for reader.HasNext() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		msg, err = reader.Next(ctx)
		cancel()
		if err != nil {
			return
		}
		if last, err = ToCheckpoint(msg); err != nil {
			return
		}
	}
	return
}

func (p *PulsarTracker) Commit(_ Checkpoint, _ pulsar.MessageID) error {
	return nil
}

func (p *PulsarTracker) Close() {
	// Do Nothing
}
