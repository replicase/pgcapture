package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/rueian/pgcapture/pkg/dblog"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/source"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const TestSlot = "dblog"

func main() {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://postgres@127.0.0.1/postgres?sslmode=disable")
	if err != nil {
		panic(err)
	}
	defer conn.Close(ctx)

	conn.Exec(ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public")
	conn.Exec(ctx, "DROP EXTENSION IF EXISTS pgcapture")
	conn.Exec(ctx, "CREATE TABLE t1 (id int primary key, txt text)")
	conn.Exec(ctx, fmt.Sprintf("select pg_drop_replication_slot('%s')", TestSlot))

	server := dblog.Gateway{
		SourceResolver: &PGXSourceResolver{},
		SourceDumper:   &dblog.PGXSourceDumper{Conn: conn},
		DumpInfoPuller: &dblog.GRPCDumpInfoPuller{Client: &ctrl{m: make(chan struct{})}},
	}

	fmt.Println(server.Capture(&logging{}))
}

type logging struct {
	state       int
	checkpoints chan *pb.CaptureMessage
}

func (l *logging) Send(message *pb.CaptureMessage) error {
	fmt.Println("Sent", message.String())
	l.checkpoints <- message
	return nil
}

func (l *logging) Recv() (*pb.CaptureRequest, error) {
	if l.state == 0 {
		l.state = 1
		l.checkpoints = make(chan *pb.CaptureMessage)
		return &pb.CaptureRequest{Type: &pb.CaptureRequest_Init{Init: &pb.CaptureInit{}}}, nil
	}
	m := <-l.checkpoints
	return &pb.CaptureRequest{Type: &pb.CaptureRequest_Ack{Ack: &pb.CaptureAck{Checkpoint: m.Checkpoint}}}, nil
}

func (l *logging) SetHeader(md metadata.MD) error {
	panic("implement me")
}

func (l *logging) SendHeader(md metadata.MD) error {
	panic("implement me")
}

func (l *logging) SetTrailer(md metadata.MD) {
	panic("implement me")
}

func (l *logging) Context() context.Context {
	return context.Background()
}

func (l *logging) SendMsg(m interface{}) error {
	panic("implement me")
}

func (l *logging) RecvMsg(m interface{}) error {
	panic("implement me")
}

type PGXSourceResolver struct {
}

func (r *PGXSourceResolver) Resolve(ctx context.Context, uri string) (source.RequeueSource, error) {
	return &source.PGXSource{
		SetupConnStr: "postgres://postgres@127.0.0.1/postgres?sslmode=disable",
		ReplConnStr:  "postgres://postgres@127.0.0.1/postgres?replication=database",
		ReplSlot:     TestSlot,
		CreateSlot:   true,
	}, nil
}

type ctrl struct {
	m chan struct{}
}

func (c *ctrl) Send(request *pb.DumpInfoRequest) error {
	fmt.Println("REQ", request.String())
	go func() {
		c.m <- struct{}{}
	}()
	return nil
}

func (c *ctrl) Recv() (*pb.DumpInfoResponse, error) {
	<-c.m
	time.Sleep(time.Second)
	return &pb.DumpInfoResponse{
		Source:     "",
		Namespace:  "public",
		Table:      "t1",
		PageStart:  0,
		PageBefore: 0,
	}, nil
}

func (c *ctrl) Header() (metadata.MD, error) {
	panic("implement me")
}

func (c *ctrl) Trailer() metadata.MD {
	panic("implement me")
}

func (c *ctrl) CloseSend() error {
	panic("implement me")
}

func (c *ctrl) Context() context.Context {
	return context.Background()
}

func (c *ctrl) SendMsg(m interface{}) error {
	panic("implement me")
}

func (c *ctrl) RecvMsg(m interface{}) error {
	panic("implement me")
}

func (c *ctrl) PullDumpInfo(ctx context.Context, opts ...grpc.CallOption) (pb.DBLogController_PullDumpInfoClient, error) {
	return c, nil
}
