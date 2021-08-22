package dblog

import (
	"context"
	"errors"
	"testing"

	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/source"
	"google.golang.org/grpc/metadata"
)

func TestGateway_CaptureInitError(t *testing.T) {
	gw := Gateway{
		SourceResolver: &resolver{
			SourceCB: func(ctx context.Context, uri string) (source.RequeueSource, error) {
				return nil, context.Canceled
			},
		},
	}
	if err := gw.Capture(&capture{
		ctx: context.Background(),
		RecvCB: func() (*pb.CaptureRequest, error) {
			return nil, context.Canceled
		},
	}); err != context.Canceled {
		t.Fatal("unexpected")
	}

	if err := gw.Capture(&capture{
		ctx: context.Background(),
		RecvCB: func() (*pb.CaptureRequest, error) {
			return &pb.CaptureRequest{}, nil
		},
	}); err != ErrCaptureInitMessageRequired {
		t.Fatal("unexpected")
	}

	if err := gw.Capture(&capture{
		ctx: context.Background(),
		RecvCB: func() (*pb.CaptureRequest, error) {
			return &pb.CaptureRequest{Type: &pb.CaptureRequest_Init{Init: &pb.CaptureInit{Uri: URI1}}}, nil
		},
	}); err != context.Canceled {
		t.Fatal("unexpected")
	}

	gw = Gateway{
		SourceResolver: &resolver{
			SourceCB: func(ctx context.Context, uri string) (source.RequeueSource, error) {
				return &sources{
					CaptureCB: func(cp source.Checkpoint) (changes chan source.Change, err error) {
						return nil, context.Canceled
					},
				}, nil
			},
			DumperCB: func(ctx context.Context, uri string) (SourceDumper, error) {
				return &dumper{
					LoadDumpCB: func(minLSN uint64, info *pb.DumpInfoResponse) ([]*pb.Change, error) {
						return nil, nil
					},
				}, nil
			},
		},
	}
	if err := gw.Capture(&capture{
		ctx: context.Background(),
		RecvCB: func() (*pb.CaptureRequest, error) {
			return &pb.CaptureRequest{Type: &pb.CaptureRequest_Init{Init: &pb.CaptureInit{Uri: URI1}}}, nil
		},
	}); err != context.Canceled {
		t.Fatal("unexpected")
	}

	gw = Gateway{
		SourceResolver: &resolver{
			SourceCB: func(ctx context.Context, uri string) (source.RequeueSource, error) {
				return &sources{
					CaptureCB: func(cp source.Checkpoint) (chan source.Change, error) {
						return nil, nil
					},
					RequeueCB: func(cp source.Checkpoint) {
					},
					CommitCB: func(cp source.Checkpoint) {
					},
					StopCB: func() error {
						return nil
					},
				}, nil
			},
			DumperCB: func(ctx context.Context, uri string) (SourceDumper, error) {
				return nil, context.Canceled
			},
		},
	}
	if err := gw.Capture(&capture{
		ctx: context.Background(),
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

	dumps := make(chan DumpInfo, 1)
	dumpAcks := make(chan string, 1)

	dumpsReq := make(chan loadDumpReq, 1)
	dumpsRes := make(chan []*pb.Change, 1)

	stopped := make(chan struct{})

	gw := Gateway{
		SourceResolver: &resolver{
			SourceCB: func(ctx context.Context, uri string) (source.RequeueSource, error) {
				return &sources{
					CaptureCB: func(cp source.Checkpoint) (chan source.Change, error) {
						checkpoints <- cp
						return changes, nil
					},
					RequeueCB: func(cp source.Checkpoint) {
						requeue <- cp
					},
					CommitCB: func(cp source.Checkpoint) {
						commit <- cp
					},
					StopCB: func() error {
						close(stopped)
						return nil
					},
				}, nil
			},
			DumperCB: func(ctx context.Context, uri string) (SourceDumper, error) {
				return &dumper{
					LoadDumpCB: func(minLSN uint64, info *pb.DumpInfoResponse) ([]*pb.Change, error) {
						dumpsReq <- loadDumpReq{minLSN: minLSN, info: info}
						res := <-dumpsRes
						if res == nil {
							return nil, errors.New("fake")
						}
						if len(res) != 0 && res[0].Schema == "ErrMissingTable" {
							return nil, ErrMissingTable
						}
						return res, nil
					},
				}, nil
			},
		},
		DumpInfoPuller: &puller{
			PullCB: func(ctx context.Context, uri string) chan DumpInfo {
				if ctx != ctx1 || uri != URI1 {
					panic("unexpected")
				}
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
			panic("unexpected")
		}
	}()

	recv <- &pb.CaptureRequest{Type: &pb.CaptureRequest_Init{Init: &pb.CaptureInit{Uri: URI1}}}

	// get init empty checkpoint by source
	<-checkpoints

	// if source have changes, send it to client
	change := &pb.Change{Op: pb.Change_UPDATE}
	changes <- source.Change{Checkpoint: source.Checkpoint{LSN: 1}, Message: &pb.Message{Type: &pb.Message_Change{Change: change}}}
	if msg := <-send; msg.Checkpoint.Lsn != 1 || msg.Change != change {
		t.Fatal("unexpected")
	}

	// if client ack, ack the source
	recv <- &pb.CaptureRequest{Type: &pb.CaptureRequest_Ack{Ack: &pb.CaptureAck{Checkpoint: &pb.Checkpoint{Lsn: 1}}}}
	if cp := <-commit; cp.LSN != 1 {
		t.Fatal("unexpected")
	}

	// if client requeue, requeue the source
	recv <- &pb.CaptureRequest{Type: &pb.CaptureRequest_Ack{Ack: &pb.CaptureAck{Checkpoint: &pb.Checkpoint{Lsn: 1}, RequeueReason: "requeue"}}}
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
	dump := &pb.DumpInfoResponse{Schema: "public", Table: "t1", PageBegin: 0, PageEnd: 0}
	dumps <- DumpInfo{Resp: dump, client: &pullDumpInfoClient{acks: dumpAcks}}
	if req := <-dumpsReq; req.minLSN != 1 || req.info != dump {
		t.Fatal("unexpected")
	}
	// prepare dump content, send it out, and ack
	loaded := []*pb.Change{{Op: pb.Change_UPDATE}, {Op: pb.Change_UPDATE}, {Op: pb.Change_UPDATE}}
	dumpsRes <- loaded
	for _, change := range loaded {
		if sent := <-send; sent.Checkpoint.Lsn != 0 || sent.Change != change {
			t.Fatal("unexpected")
		}
	}
	// client ack the dump changes
	for i := range loaded {
		var isLast []byte
		if i+1 == len(loaded) {
			isLast = []byte{1}
		}
		recv <- &pb.CaptureRequest{Type: &pb.CaptureRequest_Ack{Ack: &pb.CaptureAck{Checkpoint: &pb.Checkpoint{Lsn: 0, Seq: dumpID(dump), Data: isLast}}}}
	}
	// wait for whole dump be acked to puller
	if reason := <-dumpAcks; reason != "" {
		t.Fatal("unexpected")
	}

	// if dump puller has dump, but client requeue
	dump = &pb.DumpInfoResponse{Schema: "public", Table: "t1", PageBegin: 0, PageEnd: 0}
	dumps <- DumpInfo{Resp: dump, client: &pullDumpInfoClient{acks: dumpAcks}}
	if req := <-dumpsReq; req.minLSN != 1 || req.info != dump {
		t.Fatal("unexpected")
	}
	// prepare dump content, send it out, and ack
	loaded = []*pb.Change{{Op: pb.Change_UPDATE}, {Op: pb.Change_UPDATE}, {Op: pb.Change_UPDATE}}
	dumpsRes <- loaded
	for _, change := range loaded {
		if sent := <-send; sent.Checkpoint.Lsn != 0 || sent.Change != change {
			t.Fatal("unexpected")
		}
	}
	// client requeue the dump changes
	for range loaded {
		recv <- &pb.CaptureRequest{Type: &pb.CaptureRequest_Ack{Ack: &pb.CaptureAck{Checkpoint: &pb.Checkpoint{Lsn: 0, Seq: dumpID(dump)}, RequeueReason: "clienterror"}}}
	}
	// wait for whole dump be acked to puller ONCE
	if reason := <-dumpAcks; reason != "clienterror" {
		t.Fatal("unexpected")
	}

	// if dump zero records, ack dump
	dumps <- DumpInfo{Resp: dump, client: &pullDumpInfoClient{acks: dumpAcks}}
	if req := <-dumpsReq; req.minLSN != 1 || req.info != dump {
		t.Fatal("unexpected")
	}
	dumpsRes <- []*pb.Change{}
	if reason := <-dumpAcks; reason != "" {
		t.Fatal("unexpected")
	}

	// if dump error, requeue dump
	dumps <- DumpInfo{Resp: dump, client: &pullDumpInfoClient{acks: dumpAcks}}
	if req := <-dumpsReq; req.minLSN != 1 || req.info != dump {
		t.Fatal("unexpected")
	}
	dumpsRes <- nil // make "fake"
	if reason := <-dumpAcks; reason != "fake" {
		t.Fatal("unexpected")
	}

	// if dump ErrMissingTable, ack dump
	dumps <- DumpInfo{Resp: dump, client: &pullDumpInfoClient{acks: dumpAcks}}
	if req := <-dumpsReq; req.minLSN != 1 || req.info != dump {
		t.Fatal("unexpected")
	}
	dumpsRes <- []*pb.Change{{Schema: "ErrMissingTable"}}
	if reason := <-dumpAcks; reason != "" {
		t.Fatal("unexpected")
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

	dumps := make(chan DumpInfo, 1)

	dumpsReq := make(chan loadDumpReq, 1)
	dumpsRes := make(chan []*pb.Change, 1)

	stopped := make(chan struct{})

	gw := Gateway{
		SourceResolver: &resolver{
			SourceCB: func(ctx context.Context, uri string) (source.RequeueSource, error) {
				return &sources{
					CaptureCB: func(cp source.Checkpoint) (chan source.Change, error) {
						checkpoints <- cp
						return changes, nil
					},
					RequeueCB: func(cp source.Checkpoint) {
						requeue <- cp
					},
					CommitCB: func(cp source.Checkpoint) {
						commit <- cp
					},
					StopCB: func() error {
						close(stopped)
						return nil
					},
				}, nil
			},
			DumperCB: func(ctx context.Context, uri string) (SourceDumper, error) {
				return &dumper{
					LoadDumpCB: func(minLSN uint64, info *pb.DumpInfoResponse) ([]*pb.Change, error) {
						dumpsReq <- loadDumpReq{minLSN: minLSN, info: info}
						return <-dumpsRes, nil
					},
				}, nil
			},
		},
		DumpInfoPuller: &puller{
			PullCB: func(ctx context.Context, uri string) chan DumpInfo {
				if ctx != ctx1 || uri != URI1 {
					t.Fatal("unexpected")
				}
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
			panic("unexpected")
		}
	}()

	recv <- &pb.CaptureRequest{Type: &pb.CaptureRequest_Init{Init: &pb.CaptureInit{Uri: URI1}}}
	// get init empty checkpoint by source
	<-checkpoints

	// if source have changes, send it to client
	change := &pb.Change{Op: pb.Change_UPDATE}
	changes <- source.Change{Checkpoint: source.Checkpoint{LSN: 1}, Message: &pb.Message{Type: &pb.Message_Change{Change: change}}}
	if msg := <-send; msg.Checkpoint.Lsn != 1 || msg.Change != change {
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

	dumps := make(chan DumpInfo, 1)

	dumpsReq := make(chan loadDumpReq, 1)
	dumpsRes := make(chan []*pb.Change, 1)

	stopped := make(chan struct{})

	gw := Gateway{
		SourceResolver: &resolver{
			SourceCB: func(ctx context.Context, uri string) (source.RequeueSource, error) {
				return &sources{
					CaptureCB: func(cp source.Checkpoint) (chan source.Change, error) {
						checkpoints <- cp
						return changes, nil
					},
					RequeueCB: func(cp source.Checkpoint) {
						requeue <- cp
					},
					CommitCB: func(cp source.Checkpoint) {
						commit <- cp
					},
					StopCB: func() error {
						close(stopped)
						return nil
					},
				}, nil
			},
			DumperCB: func(ctx context.Context, uri string) (SourceDumper, error) {
				return &dumper{
					LoadDumpCB: func(minLSN uint64, info *pb.DumpInfoResponse) ([]*pb.Change, error) {
						dumpsReq <- loadDumpReq{minLSN: minLSN, info: info}
						return <-dumpsRes, nil
					},
				}, nil
			},
		},
		DumpInfoPuller: &puller{
			PullCB: func(ctx context.Context, uri string) chan DumpInfo {
				if ctx != ctx1 || uri != URI1 {
					t.Fatal("unexpected")
				}
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
			panic("unexpected")
		}
	}()

	recv <- &pb.CaptureRequest{Type: &pb.CaptureRequest_Init{Init: &pb.CaptureInit{Uri: URI1}}}
	// get init empty checkpoint by source
	<-checkpoints

	// if dump puller has dump
	dump := &pb.DumpInfoResponse{Schema: "public", Table: "t1", PageBegin: 0, PageEnd: 0}
	dumps <- DumpInfo{Resp: dump}
	if req := <-dumpsReq; req.minLSN != 0 || req.info != dump {
		t.Fatal("unexpected")
	}

	// prepare dump content
	loaded := []*pb.Change{{Op: pb.Change_UPDATE}}
	dumpsRes <- loaded
	if sent := <-send; sent.Checkpoint.Lsn != 0 || sent.Change != loaded[0] {
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

	dumps := make(chan DumpInfo, 1)

	dumpsReq := make(chan loadDumpReq, 1)
	dumpsRes := make(chan []*pb.Change, 1)

	stopped := make(chan struct{})

	gw := Gateway{
		SourceResolver: &resolver{
			SourceCB: func(ctx context.Context, uri string) (source.RequeueSource, error) {
				return &sources{
					CaptureCB: func(cp source.Checkpoint) (chan source.Change, error) {
						checkpoints <- cp
						return changes, nil
					},
					RequeueCB: func(cp source.Checkpoint) {
						requeue <- cp
					},
					CommitCB: func(cp source.Checkpoint) {
						commit <- cp
					},
					StopCB: func() error {
						close(stopped)
						return nil
					},
				}, nil
			},
			DumperCB: func(ctx context.Context, uri string) (SourceDumper, error) {
				return &dumper{
					LoadDumpCB: func(minLSN uint64, info *pb.DumpInfoResponse) ([]*pb.Change, error) {
						dumpsReq <- loadDumpReq{minLSN: minLSN, info: info}
						return <-dumpsRes, nil
					},
				}, nil
			},
		},
		DumpInfoPuller: &puller{
			PullCB: func(ctx context.Context, uri string) chan DumpInfo {
				if ctx != ctx1 || uri != URI1 {
					t.Fatal("unexpected")
				}
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
			panic("unexpected")
		}
	}()

	recv <- &pb.CaptureRequest{Type: &pb.CaptureRequest_Init{Init: &pb.CaptureInit{Uri: URI1}}}
	// get init empty checkpoint by source
	<-checkpoints
	close(recv)
	<-stopped
}

func TestGateway_CaptureRecvClose(t *testing.T) {
	ctx1 := context.Background()

	changes := make(chan source.Change, 1)
	checkpoints := make(chan source.Checkpoint, 1)
	requeue := make(chan source.Checkpoint, 1)
	commit := make(chan source.Checkpoint, 1)

	recv := make(chan *pb.CaptureRequest, 1)
	send := make(chan *pb.CaptureMessage, 1)

	dumps := make(chan DumpInfo, 1)

	dumpsReq := make(chan loadDumpReq, 1)
	dumpsRes := make(chan []*pb.Change, 1)

	stopped := make(chan struct{})

	gw := Gateway{
		SourceResolver: &resolver{
			SourceCB: func(ctx context.Context, uri string) (source.RequeueSource, error) {
				return &sources{
					CaptureCB: func(cp source.Checkpoint) (chan source.Change, error) {
						checkpoints <- cp
						return changes, nil
					},
					RequeueCB: func(cp source.Checkpoint) {
						requeue <- cp
					},
					CommitCB: func(cp source.Checkpoint) {
						commit <- cp
					},
					StopCB: func() error {
						close(stopped)
						return nil
					},
				}, nil
			},
			DumperCB: func(ctx context.Context, uri string) (SourceDumper, error) {
				return &dumper{
					LoadDumpCB: func(minLSN uint64, info *pb.DumpInfoResponse) ([]*pb.Change, error) {
						dumpsReq <- loadDumpReq{minLSN: minLSN, info: info}
						return <-dumpsRes, nil
					},
				}, nil
			},
		},
		DumpInfoPuller: &puller{
			PullCB: func(ctx context.Context, uri string) chan DumpInfo {
				if ctx != ctx1 || uri != URI1 {
					t.Fatal("unexpected")
				}
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
		}); err != nil {
			panic("unexpected")
		}
	}()

	recv <- &pb.CaptureRequest{Type: &pb.CaptureRequest_Init{Init: &pb.CaptureInit{Uri: URI1}}}
	// get init empty checkpoint by source
	<-checkpoints
	close(changes)
	<-stopped
	close(recv)
}

type puller struct {
	PullCB func(ctx context.Context, uri string) chan DumpInfo
}

func (p *puller) Pull(ctx context.Context, uri string) chan DumpInfo {
	return p.PullCB(ctx, uri)
}

type dumper struct {
	LoadDumpCB func(minLSN uint64, info *pb.DumpInfoResponse) ([]*pb.Change, error)
}

func (d *dumper) LoadDump(minLSN uint64, info *pb.DumpInfoResponse) ([]*pb.Change, error) {
	return d.LoadDumpCB(minLSN, info)
}

func (d *dumper) Stop() {

}

type sources struct {
	CaptureCB func(cp source.Checkpoint) (changes chan source.Change, err error)
	CommitCB  func(cp source.Checkpoint)
	StopCB    func() error
	RequeueCB func(cp source.Checkpoint)
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

func (s *sources) Stop() error {
	return s.StopCB()
}

func (s *sources) Requeue(cp source.Checkpoint, reason string) {
	s.RequeueCB(cp)
}

type resolver struct {
	SourceCB func(ctx context.Context, uri string) (source.RequeueSource, error)
	DumperCB func(ctx context.Context, uri string) (SourceDumper, error)
}

func (r *resolver) Source(ctx context.Context, uri string) (source.RequeueSource, error) {
	return r.SourceCB(ctx, uri)
}

func (r *resolver) Dumper(ctx context.Context, uri string) (SourceDumper, error) {
	return r.DumperCB(ctx, uri)
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

type pullDumpInfoClient struct {
	acks chan string
}

func (p *pullDumpInfoClient) Send(request *pb.DumpInfoRequest) error {
	p.acks <- request.RequeueReason
	return nil
}

func (p *pullDumpInfoClient) Recv() (*pb.DumpInfoResponse, error) {
	panic("implement me")
}

func (p *pullDumpInfoClient) Header() (metadata.MD, error) {
	panic("implement me")
}

func (p *pullDumpInfoClient) Trailer() metadata.MD {
	panic("implement me")
}

func (p *pullDumpInfoClient) CloseSend() error {
	panic("implement me")
}

func (p *pullDumpInfoClient) Context() context.Context {
	panic("implement me")
}

func (p *pullDumpInfoClient) SendMsg(m interface{}) error {
	panic("implement me")
}

func (p *pullDumpInfoClient) RecvMsg(m interface{}) error {
	panic("implement me")
}
