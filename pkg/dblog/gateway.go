package dblog

import (
	"errors"
	"regexp"

	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/pgcapture"
	"github.com/rueian/pgcapture/pkg/source"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/peer"
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

	filter, err := tableRegexFromInit(init)
	if err != nil {
		return err
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

	return s.capture(init, filter, server, src, dumper)
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

func (s *Gateway) capture(init *pb.CaptureInit, filter *regexp.Regexp, server pb.DBLogGateway_CaptureServer, src source.RequeueSource, dumper SourceDumper) error {
	var addr string
	if p, ok := peer.FromContext(server.Context()); ok {
		addr = p.Addr.String()
	}
	logger := logrus.WithFields(logrus.Fields{"URI": init.Uri, "From": "Gateway", "Peer": addr})

	changes, err := src.Capture(source.Checkpoint{})
	if err != nil {
		return err
	}
	defer func() {
		src.Stop()
		logger.Infof("stop capturing")
	}()
	logger.Infof("start capturing")

	done := s.acknowledge(server, src)
	dumps := s.DumpInfoPuller.Pull(server.Context(), init.Uri)

	lsn := uint64(0)

	for {
		select {
		case <-server.Context().Done():
			return server.Context().Err()
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
			if filter == nil || filter.MatchString(info.Resp.Table) {
				dump, err := dumper.LoadDump(lsn, info.Resp)
				if err == nil {
					logger.WithFields(logrus.Fields{"Dump": info.Resp.String(), "Len": len(dump)}).Info("dump loaded")
				} else {
					logger.WithFields(logrus.Fields{"Dump": info.Resp.String()}).Errorf("dump error %v", err)
				}
				for i, change := range dump {
					if err = server.Send(&pb.CaptureMessage{Checkpoint: &pb.Checkpoint{Lsn: 0}, Change: change}); err != nil {
						logger.WithFields(logrus.Fields{"Dump": info.Resp.String(), "Len": len(dump), "Idx": i}).Errorf("partial dump error: %v", err)
						info.Ack(err.Error())
						return err
					}
				}
				if err != nil && err != ErrMissingTable {
					info.Ack(err.Error())
					continue
				}
			}
			info.Ack("")
		case err := <-done:
			return err
		}
	}
}

func tableRegexFromInit(init *pb.CaptureInit) (*regexp.Regexp, error) {
	if init.Parameters == nil || init.Parameters.Fields == nil {
		return nil, nil
	}
	if regex, ok := init.Parameters.Fields[pgcapture.TableRegexOption]; ok {
		return regexp.Compile(regex.GetStringValue())
	}
	return nil, nil
}

var (
	ErrCaptureInitMessageRequired = errors.New("the first request should be a CaptureInit message")
)
