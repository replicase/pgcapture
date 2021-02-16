package source

import (
	"context"
	"errors"
	"github.com/rueian/pgcapture/pkg/pb"
	"net"
	"sync/atomic"
	"time"
)

type Checkpoint struct {
	LSN  uint64
	Time time.Time
}

type Change struct {
	Checkpoint Checkpoint
	Message    *pb.Message
}

type Source interface {
	Setup() error
	Capture(cp Checkpoint) (changes chan Change, err error)
	Commit(cp Checkpoint)
	Stop()
}

type BaseSource struct {
	stop    int64
	stopped chan struct{}

	err error
}

func (b *BaseSource) Setup() error {
	panic("implement me")
}

func (b *BaseSource) Capture(cp Checkpoint) (changes chan Change, err error) {
	panic("implement me")
}

func (b *BaseSource) Commit(cp Checkpoint) {
	panic("implement me")
}

func (b *BaseSource) Stop() {
	atomic.StoreInt64(&b.stop, 1)
	if b.stopped != nil {
		<-b.stopped
	}
}

func (b *BaseSource) Error() error {
	return b.err
}

func (b *BaseSource) capture(readFn ReadFn, flushFn FlushFn) (chan Change, error) {
	b.stopped = make(chan struct{})
	changes := make(chan Change, 100)
	go func() {
		defer close(b.stopped)
		defer close(changes)
		defer flushFn()
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			change, err := readFn(ctx)
			cancel()
			if atomic.LoadInt64(&b.stop) != 0 {
				return
			}
			if isTimeout(err) {
				continue
			}
			if err != nil {
				b.err = err
				return
			}
			if change.Message != nil {
				changes <- change
			}
		}
	}()
	return changes, nil
}

type CaptureFn func(changes chan Change) error
type FlushFn func()
type ReadFn func(ctx context.Context) (Change, error)

func isTimeout(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
