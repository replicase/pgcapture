package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgtype"
	"github.com/rueian/pgcapture/example"
	"github.com/rueian/pgcapture/pkg/dblog"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/pgcapture"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

type T1 struct {
	ID pgtype.Int4 `pg:"id"`
	V  pgtype.Int4 `pg:"v"`
}

func (t *T1) TableName() (schema, table string) {
	return "public", example.TestTable
}

func main() {
	conn, err := grpc.Dial(example.GatewayAddr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	parameters, err := structpb.NewStruct(map[string]interface{}{
		dblog.TableRegexOption: example.TestTable,
	})
	if err != nil {
		panic(err)
	}

	consumer := pgcapture.NewConsumer(context.Background(), conn, &pb.CaptureInit{
		Uri:        example.SrcDB.DB,
		Parameters: parameters,
	})
	defer consumer.Stop()

	err = consumer.Consume(map[pgcapture.Model]pgcapture.ModelHandlerFunc{
		&T1{}: func(change pgcapture.Change) error {
			fmt.Println(change.New)
			return nil
		},
	})
	if err != nil {
		panic(err)
	}
}
