package dblog

import (
	"context"
	"testing"

	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/source"
	"google.golang.org/grpc/metadata"
)

func TestGateway_CaptureInitError(t *testing.T) {
	gw := Gateway{
		SourceResolver: &resolver{
			ResolveCB: func(ctx context.Context, uri string) (source.RequeueSource, error) {
				return nil, context.Canceled
			},
		},
	}
	if err := gw.Capture(&capture{
		RecvCB: func() (*pb.CaptureRequest, error) {
			return nil, context.Canceled
		},
	}); err != context.Canceled {
		t.Fatal("unexpected")
	}

	if err := gw.Capture(&capture{
		RecvCB: func() (*pb.CaptureRequest, error) {
			return &pb.CaptureRequest{}, nil
		},
	}); err != ErrCaptureInitMessageRequired {
		t.Fatal("unexpected")
	}

	if err := gw.Capture(&capture{
		RecvCB: func() (*pb.CaptureRequest, error) {
			return &pb.CaptureRequest{Type: &pb.CaptureRequest_Init{Init: &pb.CaptureInit{Uri: URI1}}}, nil
		},
	}); err != context.Canceled {
		t.Fatal("unexpected")
	}

	gw = Gateway{
		SourceResolver: &resolver{
			ResolveCB: func(ctx context.Context, uri string) (source.RequeueSource, error) {
				return &sources{
					CaptureCB: func(cp source.Checkpoint) (changes chan source.Change, err error) {
						return nil, context.Canceled
					},
				}, nil
			},
		},
	}
	if err := gw.Capture(&capture{
		RecvCB: func() (*pb.CaptureRequest, error) {
			return &pb.CaptureRequest{Type: &pb.CaptureRequest_Init{Init: &pb.CaptureInit{Uri: URI1}}}, nil
		},
	}); err != context.Canceled {
		t.Fatal("unexpected")
	}
}

type loadDumpReq struct {
	minLSN uint64
	info   *pb.DumpInfoResponse
}

func TestGateway_Capture(t *testing.T) {
	ctx1 := context.Background()

	changes := make(chan source.Change, 1)
	checkpoints := make(chan source.Checkpoint, 1)
	requeue := make(chan source.Checkpoint, 1)
	commit := make(chan source.Checkpoint, 1)

	recv := make(chan *pb.CaptureRequest, 1)
	send := make(chan *pb.CaptureMessage, 1)

	dumps := make(chan *pb.DumpInfoResponse, 1)
	dumpAcks := make(chan string, 1)

	dumpsReq := make(chan loadDumpReq, 1)
	dumpsRes := make(chan []*pb.Change, 1)

	stopped := make(chan struct{})

	gw := Gateway{
		SourceResolver: &resolver{
			ResolveCB: func(ctx context.Context, uri string) (source.RequeueSource, error) {
				return &sources{
					CaptureCB: func(cp source.Checkpoint) (chan source.Change, error) {
						checkpoints <- cp
						return changes, nil
					},
					RequeueCB: func(checkpoint source.Checkpoint) {
						requeue <- checkpoint
					},
					CommitCB: func(cp source.Checkpoint) {
						commit <- cp
					},
					StopCB: func() {
						close(stopped)
					},
				}, nil
			},
		},
		SourceDumper: &dumper{
			LoadDumpCB: func(minLSN uint64, info *pb.DumpInfoResponse) ([]*pb.Change, error) {
				dumpsReq <- loadDumpReq{minLSN: minLSN, info: info}
				return <-dumpsRes, nil
			},
		},
		DumpInfoPuller: &puller{
			PullCB: func(ctx context.Context, uri string, acks chan string) chan *pb.DumpInfoResponse {
				if ctx != ctx1 || uri != URI1 {
					t.Fatal("unexpected")
				}
				go func() {
					for s := range acks {
						dumpAcks <- s
					}
				}()
				return dumps
			},
		},
	}
	go func() {
		if err := gw.Capture(&capture{
			ctx: ctx1,
			RecvCB: func() (*pb.CaptureRequest, error) {
				return <-recv, nil
			},
			SendCB: func(message *pb.CaptureMessage) error {
				send <- message
				return nil
			},
		}); err != nil {
			t.Fatal("unexpected")
		}
	}()

	recv <- &pb.CaptureRequest{Type: &pb.CaptureRequest_Init{Init: &pb.CaptureInit{Uri: URI1}}}

	// get init empty checkpoint by source
	<-checkpoints

	// if source have changes, send it to client
	change := &pb.Change{Op: pb.Change_UPDATE}
	changes <- source.Change{Checkpoint: source.Checkpoint{LSN: 1}, Message: &pb.Message{Type: &pb.Message_Change{Change: change}}}
	if msg := <-send; msg.Checkpoint != 1 || msg.Change != change {
		t.Fatal("unexpected")
	}

	// if client ack, ack the source
	recv <- &pb.CaptureRequest{Type: &pb.CaptureRequest_Ack{Ack: &pb.CaptureAck{Checkpoint: 1}}}
	if cp := <-commit; cp.LSN != 1 {
		t.Fatal("unexpected")
	}

	// if client requeue, requeue the source
	recv <- &pb.CaptureRequest{Type: &pb.CaptureRequest_Ack{Ack: &pb.CaptureAck{Checkpoint: 1, RequeueReason: "requeue"}}}
	if cp := <-requeue; cp.LSN != 1 {
		t.Fatal("unexpected")
	}

	// if source have begin, not send it to client, but ack
	changes <- source.Change{Checkpoint: source.Checkpoint{LSN: 1}, Message: &pb.Message{Type: &pb.Message_Begin{Begin: &pb.Begin{}}}}
	if cp := <-commit; cp.LSN != 1 {
		t.Fatal("unexpected")
	}

	// if source have commit, not send it to client, but ack
	changes <- source.Change{Checkpoint: source.Checkpoint{LSN: 1}, Message: &pb.Message{Type: &pb.Message_Commit{Commit: &pb.Commit{}}}}
	if cp := <-commit; cp.LSN != 1 {
		t.Fatal("unexpected")
	}

	// if dump puller has dump
	dump := &pb.DumpInfoResponse{Namespace: "public", Table: "t1", PageBegin: 0, PageEnd: 0}
	dumps <- dump
	if req := <-dumpsReq; req.minLSN != 1 || req.info != dump {
		t.Fatal("unexpected")
	}

	// prepare dump content
	loaded := []*pb.Change{{Op: pb.Change_UPDATE}, {Op: pb.Change_UPDATE}, {Op: pb.Change_UPDATE}}
	dumpsRes <- loaded
	for _, change := range loaded {
		if sent := <-send; sent.Checkpoint != 0 || sent.Change != change {
			t.Fatal("unexpected")
		}
	}
	// if client ack, ack the puller
	for range loaded {
		recv <- &pb.CaptureRequest{Type: &pb.CaptureRequest_Ack{Ack: &pb.CaptureAck{Checkpoint: 0}}}
		if reason := <-dumpAcks; reason != "" {
			t.Fatal("unexpected")
		}
	}

	// if client requeue, requeue the puller
	for range loaded {
		recv <- &pb.CaptureRequest{Type: &pb.CaptureRequest_Ack{Ack: &pb.CaptureAck{Checkpoint: 0, RequeueReason: "requeue"}}}
		if reason := <-dumpAcks; reason != "requeue" {
			t.Fatal("unexpected")
		}
	}

	close(dumps)
	<-stopped
}

func TestGateway_CaptureSendSourceError(t *testing.T) {
	ctx1 := context.Background()

	changes := make(chan source.Change, 1)
	checkpoints := make(chan source.Checkpoint, 1)
	requeue := make(chan source.Checkpoint, 1)
	commit := make(chan source.Checkpoint, 1)

	recv := make(chan *pb.CaptureRequest, 1)
	send := make(chan *pb.CaptureMessage, 1)

	dumps := make(chan *pb.DumpInfoResponse, 1)
	dumpAcks := make(chan string, 1)

	dumpsReq := make(chan loadDumpReq, 1)
	dumpsRes := make(chan []*pb.Change, 1)

	stopped := make(chan struct{})

	gw := Gateway{
		SourceResolver: &resolver{
			ResolveCB: func(ctx context.Context, uri string) (source.RequeueSource, error) {
				return &sources{
					CaptureCB: func(cp source.Checkpoint) (chan source.Change, error) {
						checkpoints <- cp
						return changes, nil
					},
					RequeueCB: func(checkpoint source.Checkpoint) {
						requeue <- checkpoint
					},
					CommitCB: func(cp source.Checkpoint) {
						commit <- cp
					},
					StopCB: func() {
						close(stopped)
					},
				}, nil
			},
		},
		SourceDumper: &dumper{
			LoadDumpCB: func(minLSN uint64, info *pb.DumpInfoResponse) ([]*pb.Change, error) {
				dumpsReq <- loadDumpReq{minLSN: minLSN, info: info}
				return <-dumpsRes, nil
			},
		},
		DumpInfoPuller: &puller{
			PullCB: func(ctx context.Context, uri string, acks chan string) chan *pb.DumpInfoResponse {
				if ctx != ctx1 || uri != URI1 {
					t.Fatal("unexpected")
				}
				go func() {
					for s := range acks {
						dumpAcks <- s
					}
				}()
				return dumps
			},
		},
	}
	go func() {
		if err := gw.Capture(&capture{
			ctx: ctx1,
			RecvCB: func() (*pb.CaptureRequest, error) {
				return <-recv, nil
			},
			SendCB: func(message *pb.CaptureMessage) error {
				send <- message
				return context.Canceled
			},
		}); err != context.Canceled {
			t.Fatal("unexpected")
		}
	}()

	recv <- &pb.CaptureRequest{Type: &pb.CaptureRequest_Init{Init: &pb.CaptureInit{Uri: URI1}}}
	// get init empty checkpoint by source
	<-checkpoints

	// if source have changes, send it to client
	change := &pb.Change{Op: pb.Change_UPDATE}
	changes <- source.Change{Checkpoint: source.Checkpoint{LSN: 1}, Message: &pb.Message{Type: &pb.Message_Change{Change: change}}}
	if msg := <-send; msg.Checkpoint != 1 || msg.Change != change {
		t.Fatal("unexpected")
	}

	<-stopped
}

func TestGateway_CaptureSendDumpError(t *testing.T) {
	ctx1 := context.Background()

	changes := make(chan source.Change, 1)
	checkpoints := make(chan source.Checkpoint, 1)
	requeue := make(chan source.Checkpoint, 1)
	commit := make(chan source.Checkpoint, 1)

	recv := make(chan *pb.CaptureRequest, 1)
	send := make(chan *pb.CaptureMessage, 1)

	dumps := make(chan *pb.DumpInfoResponse, 1)
	dumpAcks := make(chan string, 1)

	dumpsReq := make(chan loadDumpReq, 1)
	dumpsRes := make(chan []*pb.Change, 1)

	stopped := make(chan struct{})

	gw := Gateway{
		SourceResolver: &resolver{
			ResolveCB: func(ctx context.Context, uri string) (source.RequeueSource, error) {
				return &sources{
					CaptureCB: func(cp source.Checkpoint) (chan source.Change, error) {
						checkpoints <- cp
						return changes, nil
					},
					RequeueCB: func(checkpoint source.Checkpoint) {
						requeue <- checkpoint
					},
					CommitCB: func(cp source.Checkpoint) {
						commit <- cp
					},
					StopCB: func() {
						close(stopped)
					},
				}, nil
			},
		},
		SourceDumper: &dumper{
			LoadDumpCB: func(minLSN uint64, info *pb.DumpInfoResponse) ([]*pb.Change, error) {
				dumpsReq <- loadDumpReq{minLSN: minLSN, info: info}
				return <-dumpsRes, nil
			},
		},
		DumpInfoPuller: &puller{
			PullCB: func(ctx context.Context, uri string, acks chan string) chan *pb.DumpInfoResponse {
				if ctx != ctx1 || uri != URI1 {
					t.Fatal("unexpected")
				}
				go func() {
					for s := range acks {
						dumpAcks <- s
					}
				}()
				return dumps
			},
		},
	}
	go func() {
		if err := gw.Capture(&capture{
			ctx: ctx1,
			RecvCB: func() (*pb.CaptureRequest, error) {
				return <-recv, nil
			},
			SendCB: func(message *pb.CaptureMessage) error {
				send <- message
				return context.Canceled
			},
		}); err != context.Canceled {
			t.Fatal("unexpected")
		}
	}()

	recv <- &pb.CaptureRequest{Type: &pb.CaptureRequest_Init{Init: &pb.CaptureInit{Uri: URI1}}}
	// get init empty checkpoint by source
	<-checkpoints

	// if dump puller has dump
	dump := &pb.DumpInfoResponse{Namespace: "public", Table: "t1", PageBegin: 0, PageEnd: 0}
	dumps <- dump
	if req := <-dumpsReq; req.minLSN != 0 || req.info != dump {
		t.Fatal("unexpected")
	}

	// prepare dump content
	loaded := []*pb.Change{{Op: pb.Change_UPDATE}}
	dumpsRes <- loaded
	if sent := <-send; sent.Checkpoint != 0 || sent.Change != loaded[0] {
		t.Fatal("unexpected")
	}

	<-stopped
}

func TestGateway_CaptureRecvError(t *testing.T) {
	ctx1 := context.Background()

	changes := make(chan source.Change, 1)
	checkpoints := make(chan source.Checkpoint, 1)
	requeue := make(chan source.Checkpoint, 1)
	commit := make(chan source.Checkpoint, 1)

	recv := make(chan *pb.CaptureRequest, 1)
	send := make(chan *pb.CaptureMessage, 1)

	dumps := make(chan *pb.DumpInfoResponse, 1)
	dumpAcks := make(chan string, 1)

	dumpsReq := make(chan loadDumpReq, 1)
	dumpsRes := make(chan []*pb.Change, 1)

	stopped := make(chan struct{})

	gw := Gateway{
		SourceResolver: &resolver{
			ResolveCB: func(ctx context.Context, uri string) (source.RequeueSource, error) {
				return &sources{
					CaptureCB: func(cp source.Checkpoint) (chan source.Change, error) {
						checkpoints <- cp
						return changes, nil
					},
					RequeueCB: func(checkpoint source.Checkpoint) {
						requeue <- checkpoint
					},
					CommitCB: func(cp source.Checkpoint) {
						commit <- cp
					},
					StopCB: func() {
						close(stopped)
					},
				}, nil
			},
		},
		SourceDumper: &dumper{
			LoadDumpCB: func(minLSN uint64, info *pb.DumpInfoResponse) ([]*pb.Change, error) {
				dumpsReq <- loadDumpReq{minLSN: minLSN, info: info}
				return <-dumpsRes, nil
			},
		},
		DumpInfoPuller: &puller{
			PullCB: func(ctx context.Context, uri string, acks chan string) chan *pb.DumpInfoResponse {
				if ctx != ctx1 || uri != URI1 {
					t.Fatal("unexpected")
				}
				go func() {
					for s := range acks {
						dumpAcks <- s
					}
				}()
				return dumps
			},
		},
	}
	go func() {
		if err := gw.Capture(&capture{
			ctx: ctx1,
			RecvCB: func() (*pb.CaptureRequest, error) {
				ret, ok := <-recv
				if ok {
					return ret, nil
				}
				return nil, context.Canceled
			},
			SendCB: func(message *pb.CaptureMessage) error {
				send <- message
				return nil
			},
		}); err != context.Canceled {
			t.Fatal("unexpected")
		}
	}()

	recv <- &pb.CaptureRequest{Type: &pb.CaptureRequest_Init{Init: &pb.CaptureInit{Uri: URI1}}}
	// get init empty checkpoint by source
	<-checkpoints
	close(recv)
	<-stopped
}

type puller struct {
	PullCB func(ctx context.Context, uri string, acks chan string) chan *pb.DumpInfoResponse
}

func (p *puller) Pull(ctx context.Context, uri string, acks chan string) chan *pb.DumpInfoResponse {
	return p.PullCB(ctx, uri, acks)
}

type dumper struct {
	LoadDumpCB func(minLSN uint64, info *pb.DumpInfoResponse) ([]*pb.Change, error)
}

func (d *dumper) LoadDump(minLSN uint64, info *pb.DumpInfoResponse) ([]*pb.Change, error) {
	return d.LoadDumpCB(minLSN, info)
}

type sources struct {
	CaptureCB func(cp source.Checkpoint) (changes chan source.Change, err error)
	CommitCB  func(cp source.Checkpoint)
	StopCB    func()
	RequeueCB func(checkpoint source.Checkpoint)
}

func (s *sources) Capture(cp source.Checkpoint) (changes chan source.Change, err error) {
	return s.CaptureCB(cp)
}

func (s *sources) Commit(cp source.Checkpoint) {
	s.CommitCB(cp)
}

func (s *sources) Error() error {
	panic("implement me")
}

func (s *sources) Stop() {
	s.StopCB()
}

func (s *sources) Requeue(checkpoint source.Checkpoint) {
	s.RequeueCB(checkpoint)
}

type resolver struct {
	ResolveCB func(ctx context.Context, uri string) (source.RequeueSource, error)
}

func (r *resolver) Resolve(ctx context.Context, uri string) (source.RequeueSource, error) {
	return r.ResolveCB(ctx, uri)
}

type capture struct {
	ctx    context.Context
	SendCB func(message *pb.CaptureMessage) error
	RecvCB func() (*pb.CaptureRequest, error)
}

func (c *capture) Send(message *pb.CaptureMessage) error {
	return c.SendCB(message)
}

func (c *capture) Recv() (*pb.CaptureRequest, error) {
	return c.RecvCB()
}

func (c *capture) SetHeader(md metadata.MD) error {
	panic("implement me")
}

func (c *capture) SendHeader(md metadata.MD) error {
	panic("implement me")
}

func (c *capture) SetTrailer(md metadata.MD) {
	panic("implement me")
}

func (c *capture) Context() context.Context {
	return c.ctx
}

func (c *capture) SendMsg(m interface{}) error {
	panic("implement me")
}

func (c *capture) RecvMsg(m interface{}) error {
	panic("implement me")
}
