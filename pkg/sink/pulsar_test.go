package sink

import (
	"strings"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/source"
)

func newPulsarSink(topic string) *PulsarSink {
	return &PulsarSink{
		PulsarOption: pulsar.ClientOptions{URL: "pulsar://127.0.0.1:6650"},
		PulsarTopic:  topic,
	}
}

func TestPulsarSink(t *testing.T) {
	topic := time.Now().Format("20060102150405")

	sink := newPulsarSink(topic)
	cp, err := sink.Setup()
	if err != nil {
		t.Fatal(err)
	}

	// test empty checkpoint
	if cp.LSN != 0 || len(cp.Data) != 0 {
		t.Fatalf("checkpoint of empty topic should be zero")
	}

	changes := make(chan source.Change)
	committed := sink.Apply(changes)

	for i := uint64(1); i < 4; i++ {
		changes <- source.Change{Checkpoint: source.Checkpoint{LSN: i}, Message: &pb.Message{Type: &pb.Message_Commit{Commit: &pb.Commit{EndLsn: i}}}}
		if cp := <-committed; cp.LSN != i {
			t.Fatalf("unexpected %v", i)
		}
	}
	close(changes)
	if err := sink.Stop(); err != nil {
		t.Fatal("unexpected", err)
	}

	if _, more := <-changes; more {
		t.Fatal("unexpected")
	}

	// test restart from last message
	sink = newPulsarSink(topic)
	cp, err = sink.Setup()
	if err != nil {
		t.Fatal(err)
	}
	if cp.LSN != 3 {
		t.Fatalf("checkpoint of non empty topic should be last message")
	}
	if err := sink.Stop(); err != nil {
		t.Fatal("unexpected", err)
	}
}

func TestPulsarSink_DuplicatedSink(t *testing.T) {
	topic := time.Now().Format("20060102150405")

	sink1 := newPulsarSink(topic)
	if _, err := sink1.Setup(); err != nil {
		t.Fatal(err)
	}

	sink2 := newPulsarSink(topic)
	if _, err := sink2.Setup(); err == nil || !strings.Contains(err.Error(), "is already connected to topic") {
		t.Fatal("duplicated sink")
	}

	if err := sink1.Stop(); err != nil {
		t.Fatal("unexpected", err)
	}
}
