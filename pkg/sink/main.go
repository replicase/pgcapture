package sink

import (
	"sync/atomic"
	"time"

	"github.com/rueian/pgcapture/pkg/source"
)

type CleanFn func()
type ApplyFn func(message source.Change, committed chan source.Checkpoint) error

type Sink interface {
	Setup() (cp source.Checkpoint, err error)
	Apply(changes chan source.Change) (committed chan source.Checkpoint)
	Error() error
	Stop()
}

type BaseSink struct {
	CleanFn CleanFn

	committed chan source.Checkpoint

	state int64
	err   atomic.Value
}

func (b *BaseSink) Setup() (cp source.Checkpoint, err error) {
	panic("implement me")
}

func (b *BaseSink) Apply(changes chan source.Change) (committed chan source.Checkpoint) {
	panic("implement me")
}

func (b *BaseSink) apply(changes chan source.Change, applyFn ApplyFn) (committed chan source.Checkpoint) {
	if !atomic.CompareAndSwapInt64(&b.state, 0, 1) {
		return nil
	}
	b.committed = make(chan source.Checkpoint, 100)
	go func() {
		for atomic.LoadInt64(&b.state) == 1 {
			select {
			case change, more := <-changes:
				if !more {
					goto cleanup
				}
				if err := applyFn(change, b.committed); err != nil {
					b.err.Store(err)
					goto cleanup
				}
			default:
			}
		}
	cleanup:
		atomic.StoreInt64(&b.state, 3)
		go b.Stop()
		for range changes {
			// this loop should do nothing and only exit when the input channel is closed
		}
	}()
	return b.committed
}

func (b *BaseSink) Error() error {
	if err, ok := b.err.Load().(error); ok {
		return err
	}
	return nil
}

func (b *BaseSink) Stop() {
	switch atomic.SwapInt64(&b.state, 2) {
	case 0, 3:
		b.CleanFn()
		if b.committed != nil {
			close(b.committed)
		}
	case 1:
		for !atomic.CompareAndSwapInt64(&b.state, 3, 2) {
			time.Sleep(time.Millisecond * 50)
			if atomic.LoadInt64(&b.state) == 2 {
				return
			}
		}
		b.CleanFn()
		if b.committed != nil {
			close(b.committed)
		}
	default:
	}
}
