package dblog

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"

	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/sirupsen/logrus"
)

var ErrEmptyURI = errors.New("first request uri should not be empty")

func NewController(scheduler Scheduler) *Controller {
	return &Controller{
		Scheduler: scheduler,
		log:       logrus.WithFields(logrus.Fields{"From": "Controller"}),
	}
}

type Controller struct {
	pb.UnimplementedDBLogControllerServer
	Scheduler Scheduler
	clients   int64
	log       *logrus.Entry
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

	log := c.log.WithFields(logrus.Fields{"URI": uri, "Client": id})
	log.Infof("registered client %s from uri %s", id, uri)

	defer func() {
		cancel()
		log.Infof("unregistered client %s to uri %s", id, uri)
	}()

	for {
		msg, err = server.Recv()
		if err != nil {
			return err
		}
		c.Scheduler.Ack(uri, id, msg.RequeueReason)
		if msg.RequeueReason != "" {
			log.WithFields(logrus.Fields{"Reason": msg.RequeueReason}).Error("requeue")
		}
	}
}

func (c *Controller) Schedule(ctx context.Context, req *pb.ScheduleRequest) (*pb.ScheduleResponse, error) {
	log := c.log.WithFields(logrus.Fields{
		"URI":     req.Uri,
		"NumDump": len(req.Dumps),
	})
	log.Infof("start scheduling dumps of %s", req.Uri)

	if err := c.Scheduler.Schedule(req.Uri, req.Dumps, func() {
		log.Infof("finish scheduling dumps of %s", req.Uri)
	}); err != nil {
		return nil, err
	}
	return &pb.ScheduleResponse{}, nil
}

func (c *Controller) StopSchedule(ctx context.Context, req *pb.StopScheduleRequest) (*pb.StopScheduleResponse, error) {
	c.log.WithFields(logrus.Fields{"URI": req.Uri}).Infof("stop scheduling dumps of %s", req.Uri)
	c.Scheduler.StopSchedule(req.Uri)
	return &pb.StopScheduleResponse{}, nil
}

func (c *Controller) SetScheduleCoolDown(ctx context.Context, req *pb.SetScheduleCoolDownRequest) (*pb.SetScheduleCoolDownResponse, error) {
	c.log.WithFields(logrus.Fields{"URI": req.Uri}).Infof("set scheduling cool down of %s to %v", req.Uri, req.Duration.AsDuration())
	c.Scheduler.SetCoolDown(req.Uri, req.Duration.AsDuration())
	return &pb.SetScheduleCoolDownResponse{}, nil
}
