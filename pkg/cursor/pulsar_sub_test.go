package cursor

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

func newPulsarSubscriptionTracker(topic string) (*PulsarSubscriptionTracker, func(), error) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		return nil, nil, err
	}

	tracker, err := NewPulsarSubscriptionTracker(client, topic)
	if err != nil {
		client.Close()
		return nil, nil, err
	}

	closeFunc := func() {
		tracker.Close()
		client.Close()
	}
	return tracker, closeFunc, nil
}

func TestPulsarSubscriptionTracker(t *testing.T) {
	topic := time.Now().Format("20060102150405")

	tracker, cancel, err := newPulsarSubscriptionTracker(topic)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
		Name:  topic + "-producer",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	var cp Checkpoint
	var mid pulsar.MessageID
	for i := 0; i < 10; i++ {
		cp = Checkpoint{LSN: uint64(i + 100)}
		mid, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
			Key:     cp.ToKey(),
			Payload: []byte("test-" + strconv.Itoa(i)),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := tracker.Commit(cp, mid); err != nil {
		t.Fatal(err)
	}

	last, err := tracker.Last()
	if err != nil {
		t.Fatal(err)
	}
	if last.LSN != 109 {
		t.Fatalf("unexpected checkpoint.LSN: %v", last.LSN)
	}
}
