package dblog

import (
	"errors"
	"regexp"

	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/source"
)

type Gateway struct {
	pb.UnimplementedDBLogGatewayServer
	SourceResolver SourceResolver
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

	src, err := s.SourceResolver.Source(server.Context(), init.Uri)
	if err != nil {
		return err
	}
	dumper, err := s.SourceResolver.Dumper(server.Context(), init.Uri)
	if err != nil {
		return err
	}
	defer dumper.Stop()

	return s.capture(init, server, src, dumper)
}

func (s *Gateway) acknowledge(server pb.DBLogGateway_CaptureServer, src source.RequeueSource) chan error {
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
				// ignore dump changes (Checkpoint == 0), do nothing
				if ack.Checkpoint.Lsn != 0 {
					if ack.RequeueReason != "" {
						src.Requeue(source.Checkpoint{
							LSN:  ack.Checkpoint.Lsn,
							Seq:  ack.Checkpoint.Seq,
							Data: ack.Checkpoint.Data,
						}, ack.RequeueReason)
					} else {
						src.Commit(source.Checkpoint{
							LSN:  ack.Checkpoint.Lsn,
							Seq:  ack.Checkpoint.Seq,
							Data: ack.Checkpoint.Data,
						})
					}
				}
			}
		}
	}()
	return done
}

func (s *Gateway) capture(init *pb.CaptureInit, server pb.DBLogGateway_CaptureServer, src source.RequeueSource, dumper SourceDumper) error {
	changes, err := src.Capture(source.Checkpoint{})
	if err != nil {
		return err
	}
	defer src.Stop()

	acks := make(chan string)
	defer close(acks)
	done := s.acknowledge(server, src)
	dumps := s.DumpInfoPuller.Pull(server.Context(), init.Uri, acks)
	lsn := uint64(0)

	filter, err := tableRegexFromInit(init)
	if err != nil {
		return err
	}

	for {
		select {
		case msg, more := <-changes:
			if !more {
				return nil
			}
			if change := msg.Message.GetChange(); change != nil && (filter == nil || filter.MatchString(change.Table)) {
				if err := server.Send(&pb.CaptureMessage{Checkpoint: &pb.Checkpoint{
					Lsn:  msg.Checkpoint.LSN,
					Seq:  msg.Checkpoint.Seq,
					Data: msg.Checkpoint.Data,
				}, Change: change}); err != nil {
					return err
				}
			} else {
				src.Commit(source.Checkpoint{
					LSN:  msg.Checkpoint.LSN,
					Seq:  msg.Checkpoint.Seq,
					Data: msg.Checkpoint.Data,
				})
			}
			lsn = msg.Checkpoint.LSN
		case info, more := <-dumps:
			if !more {
				return nil
			}
			dump, err := dumper.LoadDump(lsn, info)
			if err == nil {
				for _, change := range dump {
					if filter == nil || filter.MatchString(change.Table) {
						if err := server.Send(&pb.CaptureMessage{Checkpoint: &pb.Checkpoint{Lsn: 0}, Change: change}); err != nil {
							return err
						}
					}
				}
				acks <- ""
			} else {
				acks <- err.Error()
			}
		case err := <-done:
			return err
		}
	}
}

const TableRegexOption = "TableRegex"

func tableRegexFromInit(init *pb.CaptureInit) (*regexp.Regexp, error) {
	if init.Parameters == nil || init.Parameters.Fields == nil {
		return nil, nil
	}
	if regex, ok := init.Parameters.Fields[TableRegexOption]; ok {
		return regexp.Compile(regex.GetStringValue())
	}
	return nil, nil
}

var (
	ErrCaptureInitMessageRequired = errors.New("the first request should be a CaptureInit message")
)
