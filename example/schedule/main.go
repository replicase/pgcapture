package main

import (
	"context"

	"github.com/rueian/pgcapture/example"
	"github.com/rueian/pgcapture/pkg/pb"
	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial(example.ControlAddr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client := pb.NewDBLogControllerClient(conn)

	pages, err := example.SinkDB.TablePages(example.TestTable)
	if err != nil {
		panic(err)
	}

	dumps := make([]*pb.DumpInfoResponse, pages)
	for i := uint32(0); i < uint32(pages); i++ {
		dumps[i] = &pb.DumpInfoResponse{Schema: "public", Table: example.TestTable, PageBegin: i, PageEnd: i}
	}
	if _, err = client.Schedule(context.Background(), &pb.ScheduleRequest{Uri: example.TestDBSrc, Dumps: dumps}); err != nil {
		panic(err)
	}
}
