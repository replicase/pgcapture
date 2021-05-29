package dblog

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"

	"github.com/rueian/pgcapture/pkg/pb"
)

var ErrEmptyURI = errors.New("first request uri should not be empty")

type Controller struct {
	pb.UnimplementedDBLogControllerServer
	Scheduler Scheduler
	clients   int64
}

func (c *Controller) PullDumpInfo(server pb.DBLogController_PullDumpInfoServer) (err error) {
	id := strconv.FormatInt(atomic.AddInt64(&c.clients, 1), 10)

	msg, err := server.Recv()
	if err != nil {
		return err
	}
	uri := msg.Uri
	if uri == "" {
		return ErrEmptyURI
	}

	cancel, err := c.Scheduler.Register(uri, id, func(dump *pb.DumpInfoResponse) error { return server.Send(dump) })
	if err != nil {
		return err
	}
	defer cancel()

	for {
		msg, err = server.Recv()
		if err != nil {
			return err
		}
		c.Scheduler.Ack(uri, id, msg.RequeueReason)
	}
}

func (c *Controller) Schedule(ctx context.Context, req *pb.ScheduleRequest) (*pb.ScheduleResponse, error) {
	if err := c.Scheduler.Schedule(req.Uri, req.Dumps, func() {

	}); err != nil {
		return nil, err
	}
	return &pb.ScheduleResponse{}, nil
}
