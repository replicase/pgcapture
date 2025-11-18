package source

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/replicase/pgcapture/internal/test"
	"github.com/replicase/pgcapture/pkg/cursor"
	"github.com/replicase/pgcapture/pkg/pb"
	"google.golang.org/protobuf/proto"
)

func TestPulsarReaderSource(t *testing.T) {
	topic := time.Now().Format("20060102150405")

	option := pulsar.ClientOptions{URL: test.GetPulsarURL()}
	client, err := pulsar.NewClient(option)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
		Name:  topic,
	})
	if err != nil {
		t.Fatal(err)
	}

	lsn := 0
	now := time.Now()

	// prepend incomplete message into topic
	incomplete, _ := proto.Marshal(&pb.Message{Type: &pb.Message_Commit{Commit: &pb.Commit{EndLsn: 111}}})
	cp := cursor.Checkpoint{LSN: 111}
	if _, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
		Key:     cp.ToKey(),
		Payload: incomplete,
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond * 200)

	for ; lsn < 3; lsn++ {
		bs, _ := proto.Marshal(&pb.Message{Type: &pb.Message_Begin{Begin: &pb.Begin{FinalLsn: uint64(lsn)}}})
		cp := cursor.Checkpoint{LSN: uint64(lsn)}
		if _, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
			Key:     cp.ToKey(),
			Payload: bs,
		}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond * 200)
	}
	producer.Flush()

	newPulsarReaderSource := func() *PulsarReaderSource {
		return &PulsarReaderSource{
			BaseSource:   BaseSource{ReadTimeout: time.Millisecond * 100},
			PulsarOption: option,
			PulsarTopic:  topic,
			seekOffset:   time.Millisecond * -100,
		}
	}

	// test from start
	src := newPulsarReaderSource()
	changes, err := src.Capture(cursor.Checkpoint{})
	if err != nil {
		t.Fatal(err)
	}

	// should not get the incomplete one
	if lsn <= 0 {
		t.Fatal("unexpected")
	}
	for i := 0; i < lsn; i++ {
		change := <-changes
		if b := change.Message.GetBegin(); b == nil || b.FinalLsn != uint64(i) {
			t.Fatalf("unexpected %v", change.Message.String())
		}
	}

	// continue to receive latest msg
	latest, _ := proto.Marshal(&pb.Message{Type: &pb.Message_Begin{Begin: &pb.Begin{FinalLsn: 3}}})
	cp = cursor.Checkpoint{LSN: 3}
	if _, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
		Key:     cp.ToKey(),
		Payload: latest,
	}); err != nil {
		t.Fatal(err)
	}
	producer.Flush()

	change := <-changes
	if b := change.Message.GetBegin(); b == nil || b.FinalLsn != 3 {
		t.Fatalf("unexpected %v", change.Message.String())
	}

	src.Stop()

	// test from specified time and lsn, and not include specified lsn
	src = newPulsarReaderSource()
	changes, err = src.Capture(cursor.Checkpoint{LSN: uint64(1), Data: []byte(now.Add(time.Millisecond * 500).Format(time.RFC3339Nano))})
	if err != nil {
		t.Fatal(err)
	}
	if lsn <= 2 {
		t.Fatal("unexpected")
	}
	for i := 2; i < lsn+1; i++ {
		change := <-changes
		if b := change.Message.GetBegin(); b == nil || b.FinalLsn != uint64(i) {
			t.Fatalf("unexpected %v", change.Message.String())
		}
	}
	src.Stop()
}

func TestPulsarConsumerSource(t *testing.T) {
	topic := time.Now().Format("20060102150405")

	option := pulsar.ClientOptions{URL: test.GetPulsarURL()}
	client, err := pulsar.NewClient(option)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:                   topic,
		Name:                    topic,
		BatchingMaxPublishDelay: 3 * time.Second,
		BatchingMaxMessages:     3,
	})
	if err != nil {
		t.Fatal(err)
	}

	newPulsarConsumerSource := func() *PulsarConsumerSource {
		return &PulsarConsumerSource{
			BaseSource:         BaseSource{ReadTimeout: time.Millisecond * 100},
			PulsarOption:       option,
			PulsarTopic:        topic,
			PulsarSubscription: topic,
		}
	}

	// test from start
	src := newPulsarConsumerSource()
	changes, err := src.Capture(cursor.Checkpoint{})
	if err != nil {
		t.Fatal(err)
	}

	messages := []*pb.Message{
		{
			Type: &pb.Message_Begin{Begin: &pb.Begin{FinalLsn: 0}},
		},
		{
			Type: &pb.Message_Change{Change: &pb.Change{Table: "1"}},
		},
		{
			Type: &pb.Message_Change{Change: &pb.Change{Table: "2"}},
		},
		{
			Type: &pb.Message_Commit{Commit: &pb.Commit{CommitLsn: 3}},
		},
	}

	for i, m := range messages {
		cp := cursor.Checkpoint{LSN: uint64(i)}
		bs, _ := proto.Marshal(m)
		producer.SendAsync(context.Background(), &pulsar.ProducerMessage{
			Key:     cp.ToKey(),
			Payload: bs,
		}, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
			if err != nil {
				t.Fatal(err)
			}
		})
	}
	producer.Flush()

	for i := 0; i < len(messages); i++ {
		change := <-changes
		msgID, err := pulsar.DeserializeMessageID(change.Checkpoint.Data)
		if err != nil {
			t.Fatal(err)
		}
		if msgID.BatchSize() > 1 && msgID.BatchSize() != int32(len(messages)-1) {
			t.Fatalf("unexpected pulsar message id batch size %v %v", msgID.BatchSize(), len(messages))
		}

		if i == 0 {
			if b := change.Message.GetBegin(); b == nil || b.FinalLsn != uint64(i) {
				t.Fatalf("unexpected begin message %v", change.Message.String())
			}
		} else if i == len(messages)-1 {
			if c := change.Message.GetCommit(); c == nil || c.CommitLsn != uint64(len(messages)-1) {
				t.Fatalf("unexpected commit message %v", change.Message.String())
			}
		} else {
			if c := change.Message.GetChange(); c == nil || c.Table != strconv.Itoa(i) {
				t.Fatalf("unexpected change message %v", change.Message.String())
			}
		}
	}
	// stop without ack
	src.Stop()

	// restart to receive same messages, and only abort the last message of the batch
	src = newPulsarConsumerSource()
	changes, err = src.Capture(cursor.Checkpoint{})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < len(messages); i++ {
		change := <-changes
		if i == 2 {
			src.Requeue(change.Checkpoint, "")
		} else {
			src.Commit(change.Checkpoint)
		}
	}
	src.Stop()

	// restart to receive same batch messages
	src = newPulsarConsumerSource()
	changes, err = src.Capture(cursor.Checkpoint{})
	if err != nil {
		t.Fatal(err)
	}

	// should only redeliver same batch messages
	for i := 0; i < len(messages)-1; i++ {
		change := <-changes
		src.Commit(change.Checkpoint)
	}

	select {
	case <-changes:
		t.Fatal("unexpected message")
	case <-time.NewTimer(time.Millisecond * 100).C:
	}
	src.Stop()
}

func TestPulsarConsumerSourceGetConsumerName(t *testing.T) {
	// test with custom consumer name set
	src := &PulsarConsumerSource{
		ConsumerName: "custom-test-consumer",
	}

	name, err := src.getConsumerName()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if name != "custom-test-consumer" {
		t.Fatalf("expected consumer name 'custom-test-consumer', got '%s'", name)
	}

	// test with empty consumer name (should fall back to hostname)
	src2 := &PulsarConsumerSource{}

	name2, err := src2.getConsumerName()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if name2 == "" {
		t.Fatal("expected non-empty hostname, got empty string")
	}

	// verify that hostname fallback returns a non-empty string
	hostname, _ := os.Hostname()
	if name2 != hostname {
		t.Fatalf("expected hostname '%s', got '%s'", hostname, name2)
	}
}
