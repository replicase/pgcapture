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

	return s.capture(init, server, src)
}

func (s *Gateway) acknowledge(server pb.DBLogGateway_CaptureServer, src source.RequeueSource, dumpAcks chan string) chan error {
	done := make(chan error)
	go func() {
		for {
			request, err := server.Recv()
			if err != nil {
				done <- err
				close(done)
				return
			}
			if ack := request.GetAck(); ack != nil {
				if ack.Checkpoint == 0 {
					dumpAcks <- ack.RequeueReason
				} else {
					if ack.RequeueReason != "" {
						src.Requeue(source.Checkpoint{LSN: ack.Checkpoint})
					} else {
						src.Commit(source.Checkpoint{LSN: ack.Checkpoint})
					}
				}
			}
		}
	}()
	return done
}

func (s *Gateway) capture(init *pb.CaptureInit, server pb.DBLogGateway_CaptureServer, src source.RequeueSource) error {
	changes, err := src.Capture(source.Checkpoint{})
	if err != nil {
		return err
	}
	defer src.Stop()

	acks := make(chan string)
	defer close(acks)
	done := s.acknowledge(server, src, acks)
	dumps := s.DumpInfoPuller.Pull(server.Context(), init.Uri, acks)
	lsn := uint64(0)

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
			} else {
				src.Commit(source.Checkpoint{LSN: msg.Checkpoint.LSN})
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
			} else {
				acks <- err.Error()
			}
		case err := <-done:
			return err
		}
	}
}

var (
	ErrCaptureInitMessageRequired = errors.New("the first request should be a CaptureInit message")
)
