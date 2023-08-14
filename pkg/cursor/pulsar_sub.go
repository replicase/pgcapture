package cursor

import (
	"context"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

var _ Tracker = (*PulsarSubscriptionTracker)(nil)

func (p *PulsarSubscriptionTracker) copyCursor() pulsar.MessageID {
	p.lock.RLock()
	defer p.lock.RUnlock()

	if p.cursor == nil {
		return nil
	}
	rst, _ := pulsar.DeserializeMessageID(p.cursor.Serialize())
	return rst
}

func equalMessageID(a pulsar.MessageID, b pulsar.MessageID) bool {
	return a.LedgerID() == b.LedgerID() &&
		a.EntryID() == b.EntryID() &&
		a.PartitionIdx() == b.PartitionIdx() &&
		a.BatchIdx() == b.BatchIdx()
}

func (p *PulsarSubscriptionTracker) tryAck() {
	current := p.copyCursor()
	if current == nil {
		return
	}
	if p.acked == nil || !equalMessageID(current, p.acked) {
		if err := p.consumer.AckIDCumulative(current); err != nil {
			return
		}
		p.acked = current
	}
}

func (p *PulsarSubscriptionTracker) waitCommit(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.tryAck()
		case <-ctx.Done():
			p.stop <- struct{}{}
			return
		}
	}
}

func NewPulsarSubscriptionTracker(client pulsar.Client, topic string, commitInterval time.Duration, replicateState bool) (*PulsarSubscriptionTracker, error) {
	if err := ensureTopic(client, topic); err != nil {
		return nil, err
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Name:                       "pulsar-subscription-tracker",
		Topic:                      topic,
		SubscriptionName:           "pulsar-subscription-tracker-consumer",
		Type:                       pulsar.Exclusive,
		ReplicateSubscriptionState: replicateState,
	})

	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	tracker := &PulsarSubscriptionTracker{
		consumer:     consumer,
		stop:         make(chan struct{}),
		lock:         sync.RWMutex{},
		commitCancel: cancel,
	}
	go tracker.waitCommit(ctx, commitInterval)
	return tracker, nil
}

type PulsarSubscriptionTracker struct {
	consumer     pulsar.Consumer
	lock         sync.RWMutex
	cursor       pulsar.MessageID
	commitCancel context.CancelFunc
	stop         chan struct{}
	acked        pulsar.MessageID
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
	p.lock.Lock()
	defer p.lock.Unlock()
	p.cursor = mid
	return nil
}

func (p *PulsarSubscriptionTracker) Close() {
	if p.consumer != nil {
		p.commitCancel()
		<-p.stop
		p.consumer.Close()
	}
}
