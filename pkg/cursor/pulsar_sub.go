package cursor

import (
	"context"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

var _ Tracker = (*PulsarSubscriptionTracker)(nil)

func (p *PulsarSubscriptionTracker) waitCommit() {
	count := 0
	for mid := range p.commit {
		if count = count + 1; count > p.commitHoldOff-1 {
			count = 0
			p.consumer.Seek(mid)
		}
	}
	p.stop <- struct{}{}
}

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

	commit := make(chan pulsar.MessageID, 10)
	stop := make(chan struct{})
	tracker := &PulsarSubscriptionTracker{
		consumer:      consumer,
		commitHoldOff: 10,
		commit:        commit,
		stop:          stop,
	}
	go tracker.waitCommit()
	return tracker, nil
}

type PulsarSubscriptionTracker struct {
	consumer      pulsar.Consumer
	commitHoldOff int
	commit        chan pulsar.MessageID
	stop          chan struct{}
}

func (p *PulsarSubscriptionTracker) read(ctx context.Context) (cp Checkpoint, err error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	msg, err := p.consumer.Receive(ctx)
	if err != nil {
		return cp, err
	}
	return ToCheckpoint(msg)
}

func (p *PulsarSubscriptionTracker) Last() (Checkpoint, error) {
	var last Checkpoint
	if p.consumer != nil {
		for {
			cp, err := p.read(context.Background())
			if err != nil {
				// most likely that there is no message in the topic
				if err == context.DeadlineExceeded {
					return last, nil
				}
				return Checkpoint{}, err
			}
			last = cp
		}
	}
	return Checkpoint{}, nil
}

func (p *PulsarSubscriptionTracker) Commit(_ Checkpoint, mid pulsar.MessageID) error {
	p.commit <- mid
	return nil
}

func (p *PulsarSubscriptionTracker) Close() {
	if p.consumer != nil {
		p.consumer.Close()
	}
	close(p.commit)
	<-p.stop
}
