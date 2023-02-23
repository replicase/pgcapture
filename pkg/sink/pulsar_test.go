package sink

import (
	"strings"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/golang/mock/gomock"
	"github.com/rueian/pgcapture/internal/cursormock"
	"github.com/rueian/pgcapture/pkg/cursor"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/source"
)

func newPulsarSink(topic string, tracker *cursormock.MockTracker) *PulsarSink {
	return &PulsarSink{
		PulsarOption: pulsar.ClientOptions{URL: "pulsar://127.0.0.1:6650"},
		PulsarTopic:  topic,
		SetupTracker: func(_ pulsar.Client, _ string) (cursor.Tracker, error) {
			return tracker, nil
		},
	}
}

func newMockTracker(ctrl *gomock.Controller) *cursormock.MockTracker {
	return cursormock.NewMockTracker(ctrl)
}

func TestPulsarSink(t *testing.T) {
	topic := time.Now().Format("20060102150405")
	ctrl := gomock.NewController(t)
	tracker := cursormock.NewMockTracker(ctrl)

	sink := newPulsarSink(topic, tracker)

	// test empty checkpoint
	tracker.EXPECT().Last().Return(cursor.Checkpoint{}, nil)
	cp, err := sink.Setup()
	if err != nil {
		t.Fatal(err)
	}

	if cp.LSN != 0 || len(cp.Data) != 0 {
		t.Fatalf("checkpoint of empty topic should be zero")
	}

	changes := make(chan source.Change)
	committed := sink.Apply(changes)

	for i := uint64(1); i < 4; i++ {
		for j := uint32(1); j < 4; j++ {
			change := source.Change{
				Checkpoint: cursor.Checkpoint{LSN: i, Seq: j},
				Message:    &pb.Message{Type: &pb.Message_Commit{Commit: &pb.Commit{EndLsn: i}}},
			}
			tracker.EXPECT().Commit(change.Checkpoint, gomock.Any()).Return(nil)
			changes <- change
			if recv := <-committed; recv.LSN != i || recv.Seq != j {
				t.Fatalf("unexpected %v", i)
			}
		}
	}
	close(changes)

	tracker.EXPECT().Close()
	if err := sink.Stop(); err != nil {
		t.Fatal("unexpected", err)
	}

	if _, more := <-changes; more {
		t.Fatal("unexpected")
	}

	// test restart from last message
	sink = newPulsarSink(topic, tracker)
	tracker.EXPECT().Last().Return(cursor.Checkpoint{LSN: 3, Seq: 3}, nil)

	cp, err = sink.Setup()
	if err != nil {
		t.Fatal(err)
	}
	if cp.LSN != 3 || cp.Seq != 3 {
		t.Fatalf("checkpoint of non empty topic should be last message")
	}

	changes = make(chan source.Change)
	committed = sink.Apply(changes)

	// test avoid duplicate publish
	for i := uint64(1); i < 4; i++ {
		for j := uint32(1); j < 4; j++ {
			// all these changes should be ignored
			changes <- source.Change{Checkpoint: cursor.Checkpoint{LSN: i, Seq: j}, Message: &pb.Message{Type: &pb.Message_Commit{Commit: &pb.Commit{EndLsn: i}}}}
		}
	}
	// these new changes should be accepted
	for i := uint64(3); i < 5; i++ {
		for j := uint32(4); j < 5; j++ {
			change := source.Change{
				Checkpoint: cursor.Checkpoint{LSN: i, Seq: j},
				Message:    &pb.Message{Type: &pb.Message_Commit{Commit: &pb.Commit{EndLsn: i}}},
			}
			tracker.EXPECT().Commit(change.Checkpoint, gomock.Any()).Return(nil)
			changes <- change
			if recv := <-committed; recv.LSN != i || recv.Seq != j {
				t.Fatalf("unexpected %v", i)
			}
		}
	}
	close(changes)

	tracker.EXPECT().Close()
	if err := sink.Stop(); err != nil {
		t.Fatal("unexpected", err)
	}
	if _, more := <-changes; more {
		t.Fatal("unexpected")
	}
}

func TestPulsarSink_DuplicatedSink(t *testing.T) {
	topic := time.Now().Format("20060102150405")
	tracker := cursormock.NewMockTracker(gomock.NewController(t))

	sink1 := newPulsarSink(topic, tracker)
	tracker.EXPECT().Last().Return(cursor.Checkpoint{}, nil)
	if _, err := sink1.Setup(); err != nil {
		t.Fatal(err)
	}

	sink2 := newPulsarSink(topic, tracker)
	if _, err := sink2.Setup(); err == nil || !strings.Contains(err.Error(), "is already connected to topic") {
		t.Fatal("should be failed with duplicated sink")
	}

	tracker.EXPECT().Close()
	if err := sink1.Stop(); err != nil {
		t.Fatal("unexpected", err)
	}
}
