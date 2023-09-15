package main

import (
	"context"

	"github.com/replicase/pgcapture/example"
	"github.com/replicase/pgcapture/pkg/pb"
	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial(example.ControlAddr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client := pgcapture.NewDBLogControllerClient(conn)

	if _, err = client.StopSchedule(context.Background(), &pb.StopScheduleRequest{Uri: example.TestDBSrc}); err != nil {
		panic(err)
	}
}
