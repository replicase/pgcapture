package dblog

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rueian/pgcapture/pkg/pb"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

func TestController_Schedule_Delegate(t *testing.T) {
	req := &pb.ScheduleRequest{Uri: URI1, Dumps: []*pb.DumpInfoResponse{{PageBegin: 1}}}
	c := &Controller{
		Scheduler: &scheduler{
			ScheduleCB: func(uri string, dumps []*pb.DumpInfoResponse) error {
				if uri != URI1 {
					t.Fatal("unexpected")
				}
				if !proto.Equal(req, &pb.ScheduleRequest{Uri: uri, Dumps: dumps}) {
					t.Fatal("unexpected")
				}
				return nil
			},
		},
	}
	if _, err := c.Schedule(context.Background(), req); err != nil {
		t.Fatal(err)
	}
}

func TestController_Schedule_Error(t *testing.T) {
	c := &Controller{
		Scheduler: &scheduler{
			ScheduleCB: func(uri string, dumps []*pb.DumpInfoResponse) error {
				return context.Canceled
			},
		},
	}
	if _, err := c.Schedule(context.Background(), &pb.ScheduleRequest{}); err != context.Canceled {
		t.Fatal(err)
	}
}

func TestController_PullDumpInfo_InitError(t *testing.T) {
	c := &Controller{}
	if err := c.PullDumpInfo(&pdis{
		RecvCB: func() (*pb.DumpInfoRequest, error) { return nil, errors.New("any") },
	}); err == nil || err.Error() != "any" {
		t.Fatal("unexpected")
	}

	if err := c.PullDumpInfo(&pdis{
		RecvCB: func() (*pb.DumpInfoRequest, error) { return &pb.DumpInfoRequest{}, nil },
	}); err != ErrEmptyURI {
		t.Fatal("unexpected")
	}
}

func TestController_PullDumpInfo_RegisterError(t *testing.T) {
	c := &Controller{Scheduler: &scheduler{
		RegisterCB: func(uri string, client string, fn OnSchedule) (CancelFunc, error) {
			return nil, errors.New("any")
		},
	}}
	if err := c.PullDumpInfo(&pdis{
		RecvCB: func() (*pb.DumpInfoRequest, error) {
			return &pb.DumpInfoRequest{Uri: URI1}, nil
		},
	}); err == nil || err.Error() != "any" {
		t.Fatal("unexpected")
	}
}

func TestController_PullDumpInfo_Delegate(t *testing.T) {
	recv := int64(0)
	acks := int64(0)
	dump := &pb.DumpInfoResponse{Table: URI1, PageBegin: 0}
	done := make(chan struct{})

	c := &Controller{Scheduler: &scheduler{
		RegisterCB: func(uri string, client string, fn OnSchedule) (CancelFunc, error) {
			if uri != URI1 {
				t.Fatal("unexpected")
			}
			if client != "1" {
				t.Fatal("unexpected")
			}
			if err := fn(dump); err != context.Canceled { // should be received in SendCB
				t.Fatal("unexpected")
			}
			return func() {
				close(done)
			}, nil
		},
		AckCB: func(uri string, client string, requeue string) {
			if uri != URI1 {
				t.Fatal("unexpected")
			}
			if client != "1" {
				t.Fatal("unexpected")
			}
			atomic.AddInt64(&acks, 1)
		},
	}}

	if err := c.PullDumpInfo(&pdis{
		RecvCB: func() (*pb.DumpInfoRequest, error) {
			if atomic.AddInt64(&recv, 1) == 100 {
				return nil, context.Canceled
			}
			return &pb.DumpInfoRequest{Uri: URI1}, nil // should be received in AckCB
		},
		SendCB: func(response *pb.DumpInfoResponse) error {
			if response != dump {
				t.Fatal("unexpected")
			}
			return context.Canceled
		},
	}); err != context.Canceled {
		t.Fatal("unexpected")
	}

	<-done

	if atomic.LoadInt64(&recv)-2 != atomic.LoadInt64(&acks) {
		t.Fatal("unexpected")
	}
}

type scheduler struct {
	ScheduleCB func(uri string, dumps []*pb.DumpInfoResponse) error
	RegisterCB func(uri string, client string, fn OnSchedule) (CancelFunc, error)
	AckCB      func(uri string, client string, requeue string)
}

func (s *scheduler) Schedule(uri string, dumps []*pb.DumpInfoResponse, fn AfterSchedule) error {
	return s.ScheduleCB(uri, dumps)
}

func (s *scheduler) Register(uri string, client string, fn OnSchedule) (CancelFunc, error) {
	return s.RegisterCB(uri, client, fn)
}

func (s *scheduler) Ack(uri string, client string, requeue string) {
	s.AckCB(uri, client, requeue)
}

func (s *scheduler) SetCoolDown(uri string, dur time.Duration) {

}

type pdis struct {
	SendCB func(response *pb.DumpInfoResponse) error
	RecvCB func() (*pb.DumpInfoRequest, error)
}

func (p *pdis) Send(response *pb.DumpInfoResponse) error {
	return p.SendCB(response)
}

func (p *pdis) Recv() (*pb.DumpInfoRequest, error) {
	return p.RecvCB()
}

func (p *pdis) SetHeader(md metadata.MD) error {
	panic("implement me")
}

func (p *pdis) SendHeader(md metadata.MD) error {
	panic("implement me")
}

func (p *pdis) SetTrailer(md metadata.MD) {
	panic("implement me")
}

func (p *pdis) Context() context.Context {
	panic("implement me")
}

func (p *pdis) SendMsg(m interface{}) error {
	panic("implement me")
}

func (p *pdis) RecvMsg(m interface{}) error {
	panic("implement me")
}
