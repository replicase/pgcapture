package source

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/jackc/pglogrepl"
	"github.com/rueian/pgcapture/pkg/pb"
)

type Checkpoint struct {
	LSN  uint64
	Seq  uint32
	Data []byte
}

func (cp *Checkpoint) Equal(cp2 Checkpoint) bool {
	return cp.LSN == cp2.LSN && cp.Seq == cp2.Seq
}

func (cp *Checkpoint) After(cp2 Checkpoint) bool {
	return (cp.LSN > cp2.LSN) || (cp.LSN == cp2.LSN && cp.Seq > cp2.Seq)
}

func (cp *Checkpoint) ToKey() string {
	return pglogrepl.LSN(cp.LSN).String() + "|" + strconv.FormatUint(uint64(cp.Seq), 16)
}

func (cp *Checkpoint) FromKey(str string) error {
	parts := strings.Split(str, "|")
	if len(parts) != 2 {
		return errors.New("malformed key, should be lsn|seq")
	}
	lsn, err := pglogrepl.ParseLSN(parts[0])
	if err != nil {
		return err
	}
	seq, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return err
	}
	cp.LSN = uint64(lsn)
	cp.Seq = uint32(seq)
	return nil
}

func ToCheckpoint(msg pulsar.Message) (cp Checkpoint, err error) {
	if err = cp.FromKey(msg.Key()); err != nil {
		return
	}
	cp.Data = msg.ID().Serialize()
	return
}

type Change struct {
	Checkpoint Checkpoint
	Message    *pb.Message
}

type Source interface {
	Capture(cp Checkpoint) (changes chan Change, err error)
	Commit(cp Checkpoint)
	Error() error
	Stop() error
}

type RequeueSource interface {
	Source
	Requeue(cp Checkpoint, reason string)
}

type BaseSource struct {
	ReadTimeout time.Duration

	state   int64
	stopped chan struct{}

	err atomic.Value
}

func (b *BaseSource) Capture(cp Checkpoint) (changes chan Change, err error) {
	panic("implement me")
}

func (b *BaseSource) Commit(cp Checkpoint) {
	panic("implement me")
}

func (b *BaseSource) Stop() error {
	switch atomic.LoadInt64(&b.state) {
	case 1, 2:
		for atomic.LoadInt64(&b.state) != 2 {
		}
		atomic.CompareAndSwapInt64(&b.state, 2, 3)
		fallthrough
	case 3:
		<-b.stopped
	}
	return b.Error()
}

func (b *BaseSource) Error() error {
	if err, ok := b.err.Load().(error); ok {
		return err
	}
	return nil
}

func (b *BaseSource) capture(readFn ReadFn, flushFn FlushFn) (chan Change, error) {
	if !atomic.CompareAndSwapInt64(&b.state, 0, 1) {
		return nil, nil
	}

	b.stopped = make(chan struct{})
	changes := make(chan Change, 1000)

	atomic.StoreInt64(&b.state, 2)

	timeout := b.ReadTimeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}

	go func() {
		defer close(b.stopped)
		defer close(changes)
		defer flushFn()
		for {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			change, err := readFn(ctx)
			cancel()
			if atomic.LoadInt64(&b.state) != 2 {
				return
			}
			if isTimeout(err) {
				continue
			}
			if err != nil {
				b.err.Store(fmt.Errorf("%w", err))
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
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
