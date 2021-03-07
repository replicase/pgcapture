package dblog

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/rueian/pgcapture/pkg/pb"
)

type DumpInfoPuller interface {
	PullDumpInfo(ctx context.Context, uri string) (chan *pb.DumpInfoResponse, error)
	Ack(err error)
}

type GRPCDumpInfoPuller struct {
	Client pb.DBLogControllerClient

	state   int64
	requeue chan string
}

func (p *GRPCDumpInfoPuller) PullDumpInfo(ctx context.Context, uri string) (chan *pb.DumpInfoResponse, error) {
	if !atomic.CompareAndSwapInt64(&p.state, 0, 1) {
		return nil, errors.New("puller is already started")
	}

	p.requeue = make(chan string)
	resp := make(chan *pb.DumpInfoResponse)

	atomic.StoreInt64(&p.state, 2)

	go func() {
		defer close(resp)
		for {
			err := p.pulling(ctx, uri, resp)
			if errors.Is(err, context.Canceled) {
				return
			}
		}
	}()

	return resp, nil
}

func (p *GRPCDumpInfoPuller) Ack(err error) {
	if atomic.LoadInt64(&p.state) == 2 {
		if err == nil {
			p.requeue <- ""
		} else {
			p.requeue <- err.Error()
		}
	}
}

func (p *GRPCDumpInfoPuller) pulling(ctx context.Context, uri string, resp chan *pb.DumpInfoResponse) error {
	server, err := p.Client.PullDumpInfo(ctx)
	if err != nil {
		return err
	}
	if err := server.Send(&pb.DumpInfoRequest{Uri: uri}); err != nil {
		return err
	}
	go func() {
		for {
			select {
			case <-server.Context().Done():
				return
			case msg := <-p.requeue:
				if err := server.Send(&pb.DumpInfoRequest{RequeueErr: msg}); err != nil {
					return
				}
			}
		}
	}()
	for {
		msg, err := server.Recv()
		if err != nil {
			return err
		}
		resp <- msg
	}
}
