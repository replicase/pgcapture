package sink

import (
	"errors"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/source"
	"testing"
	"time"
)

type sink struct {
	BaseSink
	Cleaned chan struct{}
}

func (s *sink) Setup() (cp source.Checkpoint, err error) {
	return
}

func (s *sink) Apply(changes chan source.Change) (committed chan source.Checkpoint) {
	s.Cleaned = make(chan struct{})
	return s.BaseSink.apply(changes, func(message source.Change, committed chan source.Checkpoint) error {
		if message.Message != nil {
			return ErrAny
		}
		committed <- message.Checkpoint
		return nil
	}, func() {
		close(s.Cleaned)
	})
}

var ErrAny = errors.New("error")

func TestBaseSink_Stop(t *testing.T) {
	// the sink should still consume changes after stopped, but do nothing
	sink := sink{}
	changes := make(chan source.Change)
	committed := sink.Apply(changes)

	sink.Stop()

	if _, more := <-committed; more {
		t.Fatal("committed channel should be closed")
	}
	if _, more := <-sink.Cleaned; more {
		t.Fatal("clean func should be called once")
	}

	for i := 0; i < 1000; i++ {
		select {
		case changes <- source.Change{Checkpoint: source.Checkpoint{}}:
		case <-time.NewTimer(time.Second).C:
			t.Fatal("push to changes should be still successful")
		}
	}
	close(changes)

	if sink.Error() != nil {
		t.Fatalf("unexpected %v", sink.Error())
	}
}

func TestBaseSink_Clean(t *testing.T) {
	// the clean func should be call if changes is closed
	sink := sink{}
	changes := make(chan source.Change)
	committed := sink.Apply(changes)

	count := 1000

	go func() {
		for i := 0; i < count; i++ {
			changes <- source.Change{Checkpoint: source.Checkpoint{}}
		}
		close(changes)
	}()

	for i := 0; i < count; i++ {
		<-committed
	}

	if _, more := <-committed; more {
		t.Fatal("committed channel should be closed")
	}
	if _, more := <-sink.Cleaned; more {
		t.Fatal("clean func should be called once")
	}

	if sink.Error() != nil {
		t.Fatalf("unexpected %v", sink.Error())
	}
}

func TestBaseSink_Error(t *testing.T) {
	sink := sink{}
	changes := make(chan source.Change)
	committed := sink.Apply(changes)

	// trigger error
	changes <- source.Change{Message: &pb.Message{}}

	if _, more := <-committed; more {
		t.Fatal("committed channel should be closed")
	}
	if _, more := <-sink.Cleaned; more {
		t.Fatal("clean func should be called once")
	}

	close(changes)

	if sink.Error() != ErrAny {
		t.Fatalf("unexpected %v", sink.Error())
	}
}
