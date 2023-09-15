package main

import (
	"context"
	"time"

	"github.com/replicase/pgcapture/example"
	"github.com/replicase/pgcapture/pkg/pb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

func main() {
	conn, err := grpc.Dial(example.ControlAddr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client := pgcapture.NewDBLogControllerClient(conn)

	pages, err := example.SinkDB.TablePages(example.TestTable)
	if err != nil {
		panic(err)
	}

	batch := uint32(1)
	var dumps []*pb.DumpInfoResponse

	for i := uint32(0); i < uint32(pages); i += batch {
		dumps = append(dumps, &pb.DumpInfoResponse{Schema: "public", Table: example.TestTable, PageBegin: i, PageEnd: i + batch - 1})
	}
	if _, err = client.Schedule(context.Background(), &pb.ScheduleRequest{Uri: example.TestDBSrc, Dumps: dumps}); err != nil {
		panic(err)
	}

	if _, err = client.SetScheduleCoolDown(context.Background(), &pb.SetScheduleCoolDownRequest{
		Uri: example.TestDBSrc, Duration: durationpb.New(time.Second * 5),
	}); err != nil {
		panic(err)
	}
}
