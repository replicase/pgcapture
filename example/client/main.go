package main

import (
	"context"
	"fmt"

	"github.com/rueian/pgcapture/example"
	"github.com/rueian/pgcapture/pkg/pb"
	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial(example.GatewayAddr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	client := pb.NewDBLogGatewayClient(conn)
	stream, err := client.Capture(context.Background())
	if err != nil {
		panic(err)
	}
	if err = stream.Send(&pb.CaptureRequest{Type: &pb.CaptureRequest_Init{Init: &pb.CaptureInit{Uri: example.SrcDB.DB}}}); err != nil {
		panic(err)
	}
	for {
		msg, err := stream.Recv()
		if err != nil {
			return
		}
		if err = stream.Send(&pb.CaptureRequest{Type: &pb.CaptureRequest_Ack{Ack: &pb.CaptureAck{Checkpoint: msg.Checkpoint}}}); err != nil {
			return
		}
		if msg.Checkpoint == 0 {
			fmt.Println("DUMP", msg.Change.String())
		} else {
			fmt.Println("LAST", msg.Change.String())
		}
	}
}
