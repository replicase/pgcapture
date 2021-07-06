package dblog

import (
	"context"
	"errors"

	"github.com/rueian/pgcapture/pkg/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type DumpInfoPuller interface {
	Pull(ctx context.Context, uri string) chan DumpInfo
}

type DumpInfo struct {
	Resp   *pb.DumpInfoResponse
	client pb.DBLogController_PullDumpInfoClient
}

func (i *DumpInfo) Ack(requeueReason string) error {
	if i.client == nil {
		return nil
	}
	err := i.client.Send(&pb.DumpInfoRequest{RequeueReason: requeueReason})
	i.client = nil
	return err
}

type GRPCDumpInfoPuller struct {
	Client pb.DBLogControllerClient
}

func (p *GRPCDumpInfoPuller) Pull(ctx context.Context, uri string) chan DumpInfo {
	resp := make(chan DumpInfo, 1)

	go func() {
		defer close(resp)
		for {
			err := p.pulling(ctx, uri, resp)
			if e, ok := status.FromError(err); (ok && e.Code() == codes.Canceled) || errors.Is(err, context.Canceled) {
				return
			}
		}
	}()

	return resp
}

func (p *GRPCDumpInfoPuller) pulling(ctx context.Context, uri string, resp chan DumpInfo) error {
	client, err := p.Client.PullDumpInfo(ctx)
	if err != nil {
		return err
	}
	if err = client.Send(&pb.DumpInfoRequest{Uri: uri}); err != nil {
		return err
	}
	for {
		msg, err := client.Recv()
		if err != nil {
			return err
		}
		resp <- DumpInfo{
			Resp:   msg,
			client: client,
		}
	}
}
