package cursor

import (
	"context"
	"errors"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

var _ Tracker = (*PulsarTracker)(nil)

type PulsarTracker struct {
	reader pulsar.Reader
}

func NewPulsarTracker(client pulsar.Client, topic string) (*PulsarTracker, error) {
	reader, err := client.CreateReader(pulsar.ReaderOptions{
		Topic:                   topic,
		Name:                    topic + "-producer",
		StartMessageID:          pulsar.LatestMessageID(),
		StartMessageIDInclusive: true,
	})
	if err != nil {
		return nil, err
	}
	return &PulsarTracker{reader: reader}, nil
}

func (p *PulsarTracker) Last() (last Checkpoint, err error) {
	defer p.reader.Close()

	var (
		msg pulsar.Message
	)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		msg, err = p.reader.Next(ctx)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) && !p.reader.HasNext() {
			err = nil
			return
		}
		if err != nil {
			return
		}
		if last, err = ToCheckpoint(msg); err != nil {
			return
		}
	}
}

func (p *PulsarTracker) Start() {
	// Do Nothing
}

func (p *PulsarTracker) Commit(_ Checkpoint, _ pulsar.MessageID) error {
	return nil
}

func (p *PulsarTracker) Close() {
	// Do Nothing
}
