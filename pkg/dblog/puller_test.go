package dblog

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rueian/pgcapture/pkg/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const URI1 = "URI1"

func TestPuller_Delegate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	sendCh := make(chan *pb.DumpInfoRequest, 1)
	recvCh := make(chan *pb.DumpInfoResponse, 1)

	puller := GRPCDumpInfoPuller{
		Client: &ctrlClient{
			ctx: ctx,
			SendCB: func(request *pb.DumpInfoRequest) error {
				sendCh <- request
				return nil
			},
			RecvCB: func() (*pb.DumpInfoResponse, error) {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case dump := <-recvCh:
					return dump, nil
				}
			},
		},
	}

	dumps := puller.Pull(ctx, URI1)

	init := <-sendCh
	if init.Uri != URI1 {
		t.Fatal("puller should call Send with init message")
	}

	dump := &pb.DumpInfoResponse{Table: URI1, PageBegin: 0}
	recvCh <- dump
	if dump != (<-dumps).Resp {
		t.Fatal("puller should deliver dumps to ret channel")
	}

	cancel()
	if _, more := <-dumps; more {
		t.Fatal("dumps should be closed after canceled")
	}
}

func TestPuller_RetryPull(t *testing.T) {
	count := int64(0)
	puller := GRPCDumpInfoPuller{
		Client: &ctrlClient{
			PullCB: func(ctx context.Context, opts ...grpc.CallOption) (pb.DBLogController_PullDumpInfoClient, error) {
				if atomic.AddInt64(&count, 1) == 1 {
					return nil, errors.New("any")
				}
				return nil, context.Canceled
			},
		},
	}

	dumps := puller.Pull(context.Background(), URI1)
	if _, more := <-dumps; more {
		t.Fatal("dumps should be closed after canceled")
	}
	if atomic.LoadInt64(&count) != 2 {
		t.Fatal("unexpected")
	}
}

func TestPuller_RetryInit(t *testing.T) {
	count := int64(0)
	puller := GRPCDumpInfoPuller{
		Client: &ctrlClient{
			SendCB: func(request *pb.DumpInfoRequest) error {
				if atomic.AddInt64(&count, 1) == 1 && request.Uri == URI1 {
					return errors.New("any")
				}
				return context.Canceled
			},
		},
	}

	dumps := puller.Pull(context.Background(), URI1)
	if _, more := <-dumps; more {
		t.Fatal("dumps should be closed after canceled")
	}
	if atomic.LoadInt64(&count) != 2 {
		t.Fatal("unexpected")
	}
}

func TestPuller_RetryRecv(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	count := int64(0)
	puller := GRPCDumpInfoPuller{
		Client: &ctrlClient{
			ctx:    ctx,
			SendCB: func(request *pb.DumpInfoRequest) error { return nil },
			RecvCB: func() (*pb.DumpInfoResponse, error) {
				if atomic.AddInt64(&count, 1) == 1 {
					return nil, errors.New("any")
				}
				return nil, context.Canceled
			},
		},
	}

	dumps := puller.Pull(context.Background(), URI1)
	if _, more := <-dumps; more {
		t.Fatal("dumps should be closed after canceled")
	}
	if atomic.LoadInt64(&count) != 2 {
		t.Fatal("unexpected")
	}
}

func TestPuller_SendErr(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	count := int64(0)
	puller := GRPCDumpInfoPuller{
		Client: &ctrlClient{
			ctx: ctx,
			SendCB: func(request *pb.DumpInfoRequest) error {
				if atomic.AddInt64(&count, 1) == 1 && request.Uri == URI1 {
					return nil
				}
				return errors.New("any")
			},
			RecvCB: func() (*pb.DumpInfoResponse, error) {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				default:
					return &pb.DumpInfoResponse{}, nil
				}
			},
		},
	}

	dumps := puller.Pull(context.Background(), URI1)
	resp := <-dumps
	resp.Ack("first ack should be called with SendCB")
	resp = <-dumps
	resp.Ack("second ack should also be call with SendCB even if errored")

	if atomic.LoadInt64(&count) != 3 {
		t.Fatal("unexpected")
	}

	cancel()
	if _, more := <-dumps; more {
		t.Fatal("dumps should be closed after canceled")
	}
}

func TestPuller_SendClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	count := int64(0)
	puller := GRPCDumpInfoPuller{
		Client: &ctrlClient{
			ctx: ctx,
			SendCB: func(request *pb.DumpInfoRequest) error {
				atomic.AddInt64(&count, 1)
				return nil
			},
			RecvCB: func() (*pb.DumpInfoResponse, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
		},
	}

	dumps := puller.Pull(context.Background(), URI1)

	time.Sleep(time.Millisecond * 10)

	// only call with init message
	if atomic.LoadInt64(&count) != 1 {
		t.Fatal("unexpected")
	}

	cancel()
	if _, more := <-dumps; more {
		t.Fatal("dumps should be closed after canceled")
	}
}

type ctrlClient struct {
	ctx    context.Context
	RecvCB func() (*pb.DumpInfoResponse, error)
	SendCB func(request *pb.DumpInfoRequest) error
	PullCB func(ctx context.Context, opts ...grpc.CallOption) (pb.DBLogController_PullDumpInfoClient, error)
}

func (c *ctrlClient) Send(request *pb.DumpInfoRequest) error {
	return c.SendCB(request)
}

func (c *ctrlClient) Recv() (*pb.DumpInfoResponse, error) {
	return c.RecvCB()
}

func (c *ctrlClient) Header() (metadata.MD, error) {
	panic("implement me")
}

func (c *ctrlClient) Trailer() metadata.MD {
	panic("implement me")
}

func (c *ctrlClient) CloseSend() error {
	panic("implement me")
}

func (c *ctrlClient) Context() context.Context {
	return c.ctx
}

func (c *ctrlClient) SendMsg(m interface{}) error {
	panic("implement me")
}

func (c *ctrlClient) RecvMsg(m interface{}) error {
	panic("implement me")
}

func (c *ctrlClient) PullDumpInfo(ctx context.Context, opts ...grpc.CallOption) (pb.DBLogController_PullDumpInfoClient, error) {
	if c.PullCB != nil {
		return c.PullCB(ctx, opts...)
	}
	return c, nil
}

func (c *ctrlClient) Schedule(ctx context.Context, in *pb.ScheduleRequest, opts ...grpc.CallOption) (*pb.ScheduleResponse, error) {
	panic("implement me")
}

func (c *ctrlClient) StopSchedule(ctx context.Context, request *pb.StopScheduleRequest, opts ...grpc.CallOption) (*pb.StopScheduleResponse, error) {
	panic("implement me")
}

func (c *ctrlClient) SetScheduleCoolDown(ctx context.Context, request *pb.SetScheduleCoolDownRequest, opts ...grpc.CallOption) (*pb.SetScheduleCoolDownResponse, error) {
	panic("implement me")
}
