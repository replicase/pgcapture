package dblog

import (
	"context"
	"errors"

	"github.com/rueian/pgcapture/pkg/pb"
)

type DumpInfoPuller interface {
	Pull(ctx context.Context, uri string, acks chan string) chan *pb.DumpInfoResponse
}

type GRPCDumpInfoPuller struct {
	Client pb.DBLogControllerClient
}

func (p *GRPCDumpInfoPuller) Pull(ctx context.Context, uri string, acks chan string) chan *pb.DumpInfoResponse {
	resp := make(chan *pb.DumpInfoResponse)

	go func() {
		defer close(resp)
		for {
			err := p.pulling(ctx, uri, resp, acks)
			if errors.Is(err, context.Canceled) {
				return
			}
		}
	}()

	return resp
}

func (p *GRPCDumpInfoPuller) pulling(ctx context.Context, uri string, resp chan *pb.DumpInfoResponse, acks chan string) error {
	server, err := p.Client.PullDumpInfo(ctx)
	if err != nil {
		return err
	}
	if err = server.Send(&pb.DumpInfoRequest{Uri: uri}); err != nil {
		return err
	}
	go func() {
		for {
			select {
			case <-server.Context().Done():
				return
			case msg, more := <-acks:
				if !more {
					return
				}
				if err := server.Send(&pb.DumpInfoRequest{RequeueReason: msg}); err != nil {
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
