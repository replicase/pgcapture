package pgcapture

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/rueian/pgcapture/pkg/cursor"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/source"
)

type DBLogGatewayConsumer struct {
	client pb.DBLogGatewayClient
	init   *pb.CaptureInit
	state  int64
	stream pb.DBLogGateway_CaptureClient
	ctx    context.Context
	cancel context.CancelFunc
	err    atomic.Value
}

func (c *DBLogGatewayConsumer) Capture(cp cursor.Checkpoint) (changes chan source.Change, err error) {
	stream, err := c.client.Capture(c.ctx)
	if err != nil {
		c.cancel()
		return nil, err
	}

	if err = stream.Send(&pb.CaptureRequest{Type: &pb.CaptureRequest_Init{Init: c.init}}); err != nil {
		c.cancel()
		return nil, err
	}

	c.stream = stream
	changes = make(chan source.Change, 1000)

	atomic.StoreInt64(&c.state, 1)

	go func() {
		defer close(changes)
		for {
			msg, err := stream.Recv()
			if err != nil {
				c.err.Store(fmt.Errorf("%w", err))
				return
			}
			changes <- source.Change{
				Checkpoint: cursor.Checkpoint{
					LSN:  msg.Checkpoint.Lsn,
					Seq:  msg.Checkpoint.Seq,
					Data: msg.Checkpoint.Data,
				},
				Message: &pb.Message{Type: &pb.Message_Change{Change: msg.Change}},
			}
		}
	}()

	return changes, nil
}

func (c *DBLogGatewayConsumer) Commit(cp cursor.Checkpoint) {
	if atomic.LoadInt64(&c.state) == 1 {
		if err := c.stream.Send(&pb.CaptureRequest{Type: &pb.CaptureRequest_Ack{Ack: &pb.CaptureAck{Checkpoint: &pb.Checkpoint{
			Lsn:  cp.LSN,
			Seq:  cp.Seq,
			Data: cp.Data,
		}}}}); err != nil {
			c.err.Store(fmt.Errorf("%w", err))
			c.Stop()
		}
	}
}

func (c *DBLogGatewayConsumer) Requeue(cp cursor.Checkpoint, reason string) {
	if atomic.LoadInt64(&c.state) == 1 {
		if err := c.stream.Send(&pb.CaptureRequest{Type: &pb.CaptureRequest_Ack{Ack: &pb.CaptureAck{Checkpoint: &pb.Checkpoint{
			Lsn:  cp.LSN,
			Seq:  cp.Seq,
			Data: cp.Data,
		}, RequeueReason: reason}}}); err != nil {
			c.err.Store(fmt.Errorf("%w", err))
			c.Stop()
		}
	}
}

func (c *DBLogGatewayConsumer) Error() error {
	if err, ok := c.err.Load().(error); ok {
		return err
	}
	return nil
}

func (c *DBLogGatewayConsumer) Stop() error {
	c.cancel()
	return c.Error()
}
