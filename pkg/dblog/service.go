package dblog

import (
	"context"
	"errors"
	"log"
	"sync/atomic"
	"time"

	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/source"
)

type SourceResolver interface {
	Resolve(ctx context.Context, uri string) (source.RequeueSource, error)
}

type SourceDumper interface {
	NextDumpInfo(uri string) (*DumpInfo, error)
	LoadDump(minLSN uint64, info *DumpInfo) ([]*pb.Change, error)
}

type DumpInfo struct {
	URI string
}

type Service struct {
	pb.UnimplementedDBLogServer
	SourceResolver SourceResolver
	SourceDumper   SourceDumper
}

func (s *Service) Capture(server pb.DBLog_CaptureServer) error {
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

func (s *Service) acknowledge(server pb.DBLog_CaptureServer, src source.RequeueSource) error {
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

func (s *Service) capture(init *pb.CaptureInit, server pb.DBLog_CaptureServer, changes chan source.Change) error {
	lsn := uint64(0)
	sig := int64(0)
	var nextDumpInfo *DumpInfo

	go func() {
		var err error
		for {
			select {
			case <-server.Context().Done():
				return
			default:
			}

			nextDumpInfo, err = s.SourceDumper.NextDumpInfo(init.Uri)
			if err != nil {
				// TODO
				continue
			}

			if nextDumpInfo != nil {
				atomic.StoreInt64(&sig, 1)
			}

			time.Sleep(time.Second)
		}
	}()

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
		default:
			if !atomic.CompareAndSwapInt64(&sig, 1, 0) {
				continue
			}
			dump, err := s.SourceDumper.LoadDump(lsn, nextDumpInfo)
			if err != nil {
				log.Println("fail LoadDump", err)
				continue
			}

			for _, change := range dump {
				if err := server.Send(&pb.CaptureMessage{Checkpoint: 0, Change: change}); err != nil {
					return err
				}
			}
		}
	}
}

var (
	ErrCaptureInitMessageRequired = errors.New("the first request should be a CaptureInit message")
)
