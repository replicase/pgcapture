package source

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/rueian/pgcapture/internal/test"
	"github.com/rueian/pgcapture/pkg/cursor"
	"github.com/rueian/pgcapture/pkg/pb"
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
	changes, err = src.Capture(cursor.Checkpoint{LSN: uint64(1), Data: []byte(now.Add(time.Millisecond * 200).Format(time.RFC3339Nano))})
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
		Topic: topic,
		Name:  topic,
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

	// begin, commit message should be ignored
	cp := cursor.Checkpoint{LSN: 1}
	bs, _ := proto.Marshal(&pb.Message{Type: &pb.Message_Begin{Begin: &pb.Begin{}}})
	if _, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
		Key:     cp.ToKey(),
		Payload: bs,
	}); err != nil {
		t.Fatal(err)
	}
	cp = cursor.Checkpoint{LSN: 1}
	bs, _ = proto.Marshal(&pb.Message{Type: &pb.Message_Commit{Commit: &pb.Commit{}}})
	if _, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
		Key:     cp.ToKey(),
		Payload: bs,
	}); err != nil {
		t.Fatal(err)
	}

	lsn := 0
	for ; lsn < 3; lsn++ {
		cp := cursor.Checkpoint{LSN: uint64(lsn)}
		bs, _ := proto.Marshal(&pb.Message{Type: &pb.Message_Change{Change: &pb.Change{Table: strconv.Itoa(lsn)}}})
		if _, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
			Key:     cp.ToKey(),
			Payload: bs,
		}); err != nil {
			t.Fatal(err)
		}
	}
	producer.Flush()

	for i := 0; i < lsn; i++ {
		change := <-changes
		if c := change.Message.GetChange(); c == nil || c.Table != strconv.Itoa(i) {
			t.Fatalf("unexpected %v", change.Message.String())
		}
	}
	// stop without ack
	src.Stop()

	// restart to receive same messages, and commit '0' and '2', but abort '1'
	src = newPulsarConsumerSource()
	changes, err = src.Capture(cursor.Checkpoint{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < lsn; i++ {
		change := <-changes
		if c := change.Message.GetChange(); c == nil || c.Table != strconv.Itoa(i) {
			t.Fatalf("unexpected %v", change.Message.String())
		}
		if i == 1 {
			src.Requeue(change.Checkpoint, "")
		} else {
			src.Commit(change.Checkpoint)
		}
	}
	src.Stop()

	// the '1' message should be redelivered
	src = newPulsarConsumerSource()
	changes, err = src.Capture(cursor.Checkpoint{})
	if err != nil {
		t.Fatal(err)
	}
	change := <-changes
	if c := change.Message.GetChange(); c == nil || c.Table != strconv.Itoa(1) {
		t.Fatalf("unexpected %v", change.Message.String())
	}
	select {
	case <-changes:
		t.Fatal("unexpected message")
	case <-time.NewTimer(time.Millisecond * 100).C:
	}
	src.Stop()
}
