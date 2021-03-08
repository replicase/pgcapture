package dblog

import (
	"context"
	"errors"

	"github.com/rueian/pgcapture/pkg/pb"
	"google.golang.org/grpc/peer"
)

type Controller struct {
	pb.UnimplementedDBLogControllerServer
	Scheduler Scheduler
}

func (c *Controller) PullDumpInfo(server pb.DBLogController_PullDumpInfoServer) (err error) {
	client, ok := peer.FromContext(server.Context())
	if !ok {
		return errors.New("fail to get peer info from context")
	}

	msg, err := server.Recv()
	if err != nil {
		return err
	}
	uri := msg.Uri
	if uri == "" {
		return errors.New("first request uri should not be empty")
	}

	cancel, err := c.Scheduler.Register(uri, client.Addr.String(), func(dump *pb.DumpInfoResponse) error { return server.Send(dump) })
	if err != nil {
		return err
	}
	defer cancel()

	for {
		msg, err = server.Recv()
		if err != nil {
			return err
		}
		c.Scheduler.Ack(uri, client.Addr.String(), msg.RequeueErr)
	}
}

func (c *Controller) Schedule(ctx context.Context, req *pb.ScheduleRequest) (*pb.ScheduleResponse, error) {
	if err := c.Scheduler.Schedule(req.Uri, req.Dumps); err != nil {
		return nil, err
	}
	return &pb.ScheduleResponse{}, nil
}
