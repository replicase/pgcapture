package main

import (
	"context"
	"strconv"
	"time"

	"github.com/jackc/pgtype"
	"github.com/replicase/pgcapture"
	"github.com/replicase/pgcapture/example"
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

type T2 struct {
	ID pgtype.Int4 `pg:"id"`
	V  pgtype.Int4 `pg:"v"`
}

func (t *T2) Deserialize(data []byte) error {
	return nil
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

	err = consumer.Consume2(pgcapture.Handlers{
		TableHandlers: map[pgcapture.Table]pgcapture.HandlerFunc{
			&T1{}: func(change pgcapture.Change) error {
				return nil
			},
		},
		MessageHandlers: map[pgcapture.Message]pgcapture.HandlerFunc{
			&T2{}: func(change pgcapture.Change) error {
				return nil
			},
		},
	})
	if err != nil {
		panic(err)
	}
}
