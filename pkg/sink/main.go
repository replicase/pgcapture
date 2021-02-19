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
	committed chan source.Checkpoint

	stop int64
	err  error
}

func (b *BaseSink) Setup() (cp source.Checkpoint, err error) {
	panic("implement me")
}

func (b *BaseSink) Apply(changes chan source.Change) (committed chan source.Checkpoint) {
	panic("implement me")
}

func (b *BaseSink) apply(changes chan source.Change, applyFn ApplyFn, cleanFn CleanFn) (committed chan source.Checkpoint) {
	b.committed = make(chan source.Checkpoint, 100)
	go func() {
		for !b.clean(cleanFn) {
			time.Sleep(time.Second)
		}
	}()
	go func() {
		defer b.Stop()
		// this loop should be exit only if the input channel is closed
		for change := range changes {
			if !b.stopped() {
				if b.err = applyFn(change, b.committed); b.err != nil {
					b.Stop()
				}
			}
		}
	}()
	return b.committed
}

func (b *BaseSink) Error() error {
	return b.err
}

func (b *BaseSink) Stop() {
	atomic.CompareAndSwapInt64(&b.stop, 0, 1)
}

func (b *BaseSink) stopped() (stopped bool) {
	return atomic.LoadInt64(&b.stop) != 0
}

func (b *BaseSink) clean(cleanFn CleanFn) (cleaned bool) {
	if cleaned = atomic.CompareAndSwapInt64(&b.stop, 1, 2); cleaned {
		cleanFn()
		close(b.committed)
	}
	return
}
