package test

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"

	"github.com/jackc/pgx/v4"
)

type DBURL struct {
	Host string
	DB   string
}

func (u DBURL) URL() string {
	return fmt.Sprintf("postgres://postgres@%s/%s?sslmode=disable", u.Host, u.DB)
}

func (u DBURL) Repl() string {
	return fmt.Sprintf("postgres://postgres@%s/%s?replication=database", u.Host, u.DB)
}

func RandomDB(u DBURL) (DBURL, error) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, u.URL())
	if err != nil {
		return DBURL{}, err
	}
	defer conn.Close(ctx)
	name := "test_" + strconv.FormatUint(rand.Uint64(), 10)
	if _, err := conn.Exec(ctx, fmt.Sprintf("create database %s", name)); err != nil {
		return DBURL{}, err
	}
	return DBURL{Host: u.Host, DB: name}, nil
}

func RandomData(u DBURL) error {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, u.URL())
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	if _, err = conn.Exec(ctx, "create table if not exists test (id int)"); err != nil {
		return err
	}
	_, err = conn.Exec(ctx, "insert into test select * from generate_series(1,100) as id")
	_, err = conn.Exec(ctx, "truncate test")
	return err
}
