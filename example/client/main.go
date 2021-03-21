package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgtype"
	"github.com/rueian/pgcapture/example"
	"github.com/rueian/pgcapture/pkg/eventing"
	"github.com/rueian/pgcapture/pkg/pb"
	"google.golang.org/grpc"
)

type T1 struct {
	ID pgtype.Int4 `pg:"id"`
	V  pgtype.Int4 `pg:"v"`
}

func (t *T1) Name() (namespace, table string) {
	return "public", example.TestTable
}

func main() {
	conn, err := grpc.Dial(example.GatewayAddr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	consumer := eventing.NewDBLogConsumer(context.Background(), conn, &pb.CaptureInit{Uri: example.SrcDB.DB})
	defer consumer.Stop()

	err = consumer.Consume(map[eventing.Model]eventing.ModelHandlerFunc{
		&T1{}: func(change eventing.Change) error {
			fmt.Println(change.New)
			return nil
		},
	})
	if err != nil {
		panic(err)
	}
}
