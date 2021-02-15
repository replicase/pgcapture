package sink

import (
	"github.com/rueian/pgcapture/pkg/source"
	"sync/atomic"
	"time"
)

type CleanFn func()
type ApplyFn func(message source.Change, committed chan uint64) error

type Sink interface {
	Setup() (lsn uint64, err error)
	Apply(changes chan source.Change) (committed chan uint64)
	Error() error
	Stop()
}

type BaseSink struct {
	committed chan uint64

	stopped uint64
	err     error
}

func (b *BaseSink) Setup() (lsn uint64, err error) {
	panic("implement me")
}

func (b *BaseSink) Apply(changes chan source.Change) (committed chan uint64) {
	panic("implement me")
}

func (b *BaseSink) apply(changes chan source.Change, applyFn ApplyFn, cleanFn CleanFn) (committed chan uint64) {
	b.committed = make(chan uint64, 100)
	go func() {
		for {
			select {
			case change, more := <-changes:
				if !more {
					return
				}
				if b.check(cleanFn) {
					continue // skip message, but do not return
				}
				if b.err = applyFn(change, b.committed); b.err != nil {
					b.Stop() // mark stop, but do not return
				}
			default:
				b.check(cleanFn)
				time.Sleep(time.Millisecond * 100)
			}
		}
	}()
	return b.committed
}

func (b *BaseSink) Error() error {
	return b.err
}

func (b *BaseSink) Stop() {
	atomic.StoreUint64(&b.stopped, 1)
}

func (b *BaseSink) check(cleanFn CleanFn) (stopped bool) {
	switch atomic.LoadUint64(&b.stopped) {
	case 0:
		return false
	case 1:
		cleanFn()
		close(b.committed)
		atomic.StoreUint64(&b.stopped, 2)
	}
	return true
}
