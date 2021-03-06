package dblog

import (
	"context"
	"errors"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/source"
)

type SourceResolver interface {
	Resolve(ctx context.Context, uri string) (source.RequeueSource, error)
}

type SourceDumper interface {
	LoadDump(minLSN uint64, info *pb.DumpInfoResponse) ([]*pb.Change, error)
}

type Gateway struct {
	pb.UnimplementedDBLogGatewayServer
	SourceResolver SourceResolver
	SourceDumper   SourceDumper
	DumpInfoPuller DumpInfoPuller
}

func (s *Gateway) Capture(server pb.DBLogGateway_CaptureServer) error {
	request, err := server.Recv()
	if err != nil {
		return err
	}

	init := request.GetInit()
	if init == nil {
		return ErrCaptureInitMessageRequired
	}

	src, err := s.SourceResolver.Resolve(server.Context(), init.Uri)
	if err != nil {
		return err
	}
	changes, err := src.Capture(source.Checkpoint{})
	if err != nil {
		return err
	}

	go s.acknowledge(server, src)

	return s.capture(init, server, changes)
}

func (s *Gateway) acknowledge(server pb.DBLogGateway_CaptureServer, src source.RequeueSource) error {
	defer src.Stop()
	for {
		request, err := server.Recv()
		if err != nil {
			return err
		}
		if ack := request.GetAck(); ack != nil {
			if ack.Requeue {
				src.Requeue(source.Checkpoint{LSN: ack.Checkpoint})
			} else {
				src.Commit(source.Checkpoint{LSN: ack.Checkpoint})
			}
		}
	}
}

func (s *Gateway) capture(init *pb.CaptureInit, server pb.DBLogGateway_CaptureServer, changes chan source.Change) error {
	lsn := uint64(0)

	dumps, err := s.DumpInfoPuller.PullDumpInfo(server.Context(), init.Uri)
	if err != nil {
		return err
	}

	for {
		select {
		case msg, more := <-changes:
			if !more {
				return nil
			}
			if change := msg.Message.GetChange(); change != nil {
				if err := server.Send(&pb.CaptureMessage{Checkpoint: msg.Checkpoint.LSN, Change: change}); err != nil {
					return err
				}
			}
			lsn = msg.Checkpoint.LSN
		case info, more := <-dumps:
			if !more {
				return nil
			}
			dump, err := s.SourceDumper.LoadDump(lsn, info)
			if err == nil {
				for _, change := range dump {
					if err := server.Send(&pb.CaptureMessage{Checkpoint: 0, Change: change}); err != nil {
						return err
					}
				}
			}
			s.DumpInfoPuller.Ack(err)
		}
	}
}

var (
	ErrCaptureInitMessageRequired = errors.New("the first request should be a CaptureInit message")
)
