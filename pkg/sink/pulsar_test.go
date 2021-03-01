package sink

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/source"
	"testing"
	"time"
)

func TestPulsarSink(t *testing.T) {
	topic := time.Now().Format("20060102150405")

	newPulsarSink := func() *PulsarSink {
		return &PulsarSink{
			PulsarOption: pulsar.ClientOptions{URL: "pulsar://127.0.0.1:6650"},
			PulsarTopic:  topic,
		}
	}

	sink := newPulsarSink()
	cp, err := sink.Setup()
	if err != nil {
		t.Fatal(err)
	}

	// test empty checkpoint
	if cp.LSN != 0 || !cp.Time.IsZero() {
		t.Fatalf("checkpoint of empty topic should be zero")
	}

	changes := make(chan source.Change)
	committed := sink.Apply(changes)

	for j := 0; j < 2; j++ {
		for i := uint64(1); i < 4; i++ {
			changes <- source.Change{Checkpoint: source.Checkpoint{LSN: i}, Message: &pb.Message{Type: &pb.Message_Begin{Begin: &pb.Begin{FinalLsn: i}}}}
			if j == 0 {
				if cp := <-committed; cp.LSN != i {
					t.Fatalf("unexpected %v", i)
				}
			} else {
				// duplicated changes should be ignored
				select {
				case cp := <-committed:
					t.Fatalf("unexpected %v", cp)
				case <-time.NewTimer(time.Millisecond * 5).C:
				}
			}
		}
	}
	close(changes)
	if _, more := <-changes; more {
		t.Fatal("unexpected")
	}
	time.Sleep(2 * time.Second) // wait pulsar client close

	// test restart from last message
	sink = newPulsarSink()
	cp, err = sink.Setup()
	if err != nil {
		t.Fatal(err)
	}
	if cp.LSN != 3 {
		t.Fatalf("checkpoint of non empty topic should be last message")
	}
}
