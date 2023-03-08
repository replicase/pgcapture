package cursor

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

func newPulsarTracker(topic string) (*PulsarTracker, func(), error) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		return nil, nil, err
	}

	tracker, err := NewPulsarTracker(client, topic)
	if err != nil {
		return nil, nil, err
	}

	closeFunc := func() {
		tracker.Close()
		client.Close()
	}

	return tracker, closeFunc, nil
}

func TestPulsarTracker(t *testing.T) {
	topic := time.Now().Format("20060102150405")

	tracker, cancel, err := newPulsarTracker(topic)
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

	for i := 0; i < 10; i++ {
		cp := Checkpoint{LSN: uint64(100 + i)}
		if _, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Key:     cp.ToKey(),
			Payload: []byte("test-" + strconv.Itoa(i)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	_ = producer.Flush()

	last, err := tracker.Last()
	if err != nil {
		t.Fatal(err)
	}

	if last.LSN != 109 {
		t.Fatalf("unexpected checkpoint.LSN: %v", last.LSN)
	}
}

func TestPulsarTracker_Empty(t *testing.T) {
	topic := time.Now().Format("20060102150405") + "-empty"

	tracker, cancel, err := newPulsarTracker(topic)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	last, err := tracker.Last()
	if err != nil {
		t.Fatal(err)
	}
	if last.LSN != 0 || last.Seq != 0 {
		t.Fatal("checkpoint of empty topic should be zero")
	}
}
