package sink

import (
	"errors"
	"testing"
	"time"

	"github.com/rueian/pgcapture/pkg/cursor"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/source"
)

type sink struct {
	BaseSink
	Cleaned chan struct{}
}

func (s *sink) Setup() (cp cursor.Checkpoint, err error) {
	s.Cleaned = make(chan struct{})
	s.BaseSink.CleanFn = func() {
		close(s.Cleaned)
	}
	return
}

func (s *sink) Apply(changes chan source.Change) (committed chan cursor.Checkpoint) {
	return s.BaseSink.apply(changes, func(message source.Change, committed chan cursor.Checkpoint) error {
		if message.Message != nil {
			return ErrAny
		}
		committed <- message.Checkpoint
		return nil
	})
}

var ErrAny = errors.New("error")

func TestBaseSink_Stop(t *testing.T) {
	// the sink should still consume changes after stopped, but do nothing
	sink := sink{}
	sink.Setup()
	changes := make(chan source.Change)
	committed := sink.Apply(changes)

	if err := sink.Stop(); err != nil {
		t.Fatalf("unexpected %v", err)
	}

	if _, more := <-committed; more {
		t.Fatal("committed channel should be closed")
	}
	if _, more := <-sink.Cleaned; more {
		t.Fatal("clean func should be called once")
	}

	for i := 0; i < 1000; i++ {
		select {
		case changes <- source.Change{Checkpoint: cursor.Checkpoint{}}:
		case <-time.NewTimer(time.Second).C:
			t.Fatal("push to changes should be still successful")
		}
	}
	close(changes)
}

func TestBaseSink_StopImmediate(t *testing.T) {
	sink := sink{}
	sink.Setup()
	changes := make(chan source.Change)
	go sink.Apply(changes)
	if err := sink.Stop(); err != nil {
		t.Fatalf("unexpected %v", err)
	}
	if _, more := <-sink.Cleaned; more {
		t.Fatal("clean func should be called after Stop()")
	}
}

func TestBaseSink_Clean(t *testing.T) {
	// the clean func should be call if changes is closed
	sink := sink{}
	sink.Setup()
	changes := make(chan source.Change)
	committed := sink.Apply(changes)

	count := 1000

	go func() {
		for i := 0; i < count; i++ {
			changes <- source.Change{Checkpoint: cursor.Checkpoint{}}
		}
		close(changes)
	}()

	for i := 0; i < count; i++ {
		<-committed
	}

	if _, more := <-committed; more {
		t.Fatal("committed channel should be closed")
	}

	if err := sink.Stop(); err != nil {
		t.Fatalf("unexpected %v", err)
	}

	if _, more := <-sink.Cleaned; more {
		t.Fatal("clean func should be called after Stop()")
	}
}

func TestBaseSink_Error(t *testing.T) {
	sink := sink{}
	sink.Setup()
	changes := make(chan source.Change)
	committed := sink.Apply(changes)

	// trigger error
	changes <- source.Change{Message: &pb.Message{}}

	if _, more := <-committed; more {
		t.Fatal("committed channel should be closed")
	}

	if err := sink.Stop(); !errors.Is(err, ErrAny) {
		t.Fatalf("unexpected %v", err)
	}

	if _, more := <-sink.Cleaned; more {
		t.Fatal("clean func should be called after Stop()")
	}

	close(changes)
}

func TestBaseSink_SecondApply(t *testing.T) {
	sink := sink{}
	sink.Setup()
	changes := make(chan source.Change)
	if first := sink.Apply(changes); first == nil {
		t.Fatal("should not nil if first time")
	}
	if second := sink.Apply(changes); second != nil {
		t.Fatal("should nil if second time")
	}
	sink.Stop()
}

func TestBaseSink_SetupPanic(t *testing.T) {
	defer func() { recover() }()
	s := BaseSink{}
	s.Setup()
	t.Fatal("should panic")
}

func TestBaseSink_ApplyPanic(t *testing.T) {
	defer func() { recover() }()
	s := BaseSink{}
	s.Apply(make(chan source.Change))
	t.Fatal("should panic")
}
