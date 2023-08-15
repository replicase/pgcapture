package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgtype"
	"github.com/rueian/pgcapture"
	"github.com/rueian/pgcapture/example"
	"google.golang.org/grpc"
)

type T1 struct {
	ID pgtype.Int4 `pg:"id"`
	V  pgtype.Int4 `pg:"v"`
}

func (t *T1) TableName() (schema, table string) {
	return "public", example.TestTable
}

func (t *T1) DebounceKey() string {
	return strconv.Itoa(int(t.ID.Int))
}

func main() {
	conn, err := grpc.Dial(example.GatewayAddr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	consumer := pgcapture.NewDBLogConsumer(context.Background(), conn, pgcapture.ConsumerOption{
		URI:              example.SrcDB.DB,
		TableRegex:       example.TestTable,
		DebounceInterval: time.Second,
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
