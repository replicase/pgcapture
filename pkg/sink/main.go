package sink

import (
	"sync"
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
	Stop() error
}

type BaseSink struct {
	CleanFn   CleanFn
	cleanOnce sync.Once

	committed chan source.Checkpoint
	state     int64
	err       atomic.Value
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
	atomic.StoreInt64(&b.state, 2)

	go func() {
		for atomic.LoadInt64(&b.state) == 2 {
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
		atomic.StoreInt64(&b.state, 4)
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

func (b *BaseSink) Stop() error {
	switch atomic.LoadInt64(&b.state) {
	case 0:
		b.cleanOnce.Do(b.CleanFn)
	case 1, 2:
		for {
			if atomic.LoadInt64(&b.state) == 2 {
				atomic.CompareAndSwapInt64(&b.state, 2, 3)
				break
			}
		}
		fallthrough
	case 3:
		for {
			if atomic.LoadInt64(&b.state) == 4 {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}
		fallthrough
	case 4:
		b.cleanOnce.Do(func() {
			b.CleanFn()
			close(b.committed)
		})
	}
	return b.Error()
}
