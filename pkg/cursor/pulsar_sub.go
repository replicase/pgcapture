package cursor

import (
	"context"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

var _ Tracker = (*PulsarSubscriptionTracker)(nil)

func NewPulsarSubscriptionTracker(client pulsar.Client, topic string) (*PulsarSubscriptionTracker, error) {
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Name:             "pulsar-subscription-tracker",
		Topic:            topic,
		SubscriptionName: topic + "-cursor-consumer",
		Type:             pulsar.Exclusive,
	})
	if err != nil {
		return nil, err
	}

	return &PulsarSubscriptionTracker{
		client:   client,
		consumer: consumer,
		ch:       consumer.Chan(),
	}, nil
}

type PulsarSubscriptionTracker struct {
	client   pulsar.Client
	consumer pulsar.Consumer
	ch       <-chan pulsar.ConsumerMessage
}

func (p *PulsarSubscriptionTracker) Last() (cp Checkpoint, err error) {
	if p.consumer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// TODO: should refine the implementations to check the last message
		msg, err := p.consumer.Receive(ctx)
		if err != nil {
			return cp, err
		}
		return ToCheckpoint(msg)
	}
	return
}

func (p *PulsarSubscriptionTracker) Commit(_ Checkpoint, mid pulsar.MessageID) error {
	// TODO: might not need to ack all the times
	if p.consumer != nil {
		if err := p.consumer.Seek(mid); err != nil {
			return err
		}
	}
	return nil
}

func (p *PulsarSubscriptionTracker) Close() {
	if p.consumer != nil {
		p.consumer.Close()
	}
	p.client.Close()
}
