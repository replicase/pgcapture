package sink

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rueian/pgcapture/pkg/cursor"
	"github.com/rueian/pgcapture/pkg/source"
)

type CleanFn func()
type ApplyFn func(sourceRemaining int, message source.Change, committed chan cursor.Checkpoint) error

type Sink interface {
	Setup() (cp cursor.Checkpoint, err error)
	Apply(changes chan source.Change) (committed chan cursor.Checkpoint)
	Error() error
	Stop() error
}

type BaseSink struct {
	CleanFn   CleanFn
	cleanOnce sync.Once

	committed chan cursor.Checkpoint
	state     int64
	err       atomic.Value
}

func (b *BaseSink) Setup() (cp cursor.Checkpoint, err error) {
	panic("implement me")
}

func (b *BaseSink) Apply(changes chan source.Change) (committed chan cursor.Checkpoint) {
	panic("implement me")
}

func (b *BaseSink) apply(changes chan source.Change, applyFn ApplyFn) (committed chan cursor.Checkpoint) {
	if !atomic.CompareAndSwapInt64(&b.state, 0, 1) {
		return nil
	}
	b.committed = make(chan cursor.Checkpoint, 1000)
	atomic.StoreInt64(&b.state, 2)

	go func() {
		ticker := time.NewTicker(time.Second)
		for atomic.LoadInt64(&b.state) == 2 {
			select {
			case change, more := <-changes:
				if !more {
					goto cleanup
				}
				if err := applyFn(len(changes), change, b.committed); err != nil {
					b.err.Store(fmt.Errorf("%w", err))
					goto cleanup
				}
			case <-ticker.C:
			}
		}
	cleanup:
		ticker.Stop()
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
		for !atomic.CompareAndSwapInt64(&b.state, 2, 3) {
			runtime.Gosched()
		}
		fallthrough
	case 3:
		for atomic.LoadInt64(&b.state) != 4 {
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
