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

	if _, err = client.StopSchedule(context.Background(), &pb.StopScheduleRequest{Uri: example.TestDBSrc}); err != nil {
		panic(err)
	}
}
