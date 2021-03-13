package test

import (
	"context"
	"fmt"

	"github.com/jackc/pgconn"
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

func (u DBURL) Exec(stmts ...string) error {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, u.URL())
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	for _, stmt := range stmts {
		if _, err = conn.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func (u DBURL) RandomData(table string) error {
	return u.Exec(
		fmt.Sprintf("create table if not exists %s (id serial primary key, v int)", table),
		fmt.Sprintf("insert into %s (v) select * from generate_series(1,100) as v", table),
		fmt.Sprintf("analyze %s", table),
	)
}

func (u DBURL) CleanData(table string) error {
	return u.Exec(fmt.Sprintf("delete from %s", table))
}

func (u DBURL) TablePages(table string) (pages int, err error) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, u.URL())
	if err != nil {
		return 0, err
	}
	defer conn.Close(ctx)

	if err := u.Exec("analyze " + table); err != nil {
		return 0, err
	}

	if err = conn.QueryRow(ctx, fmt.Sprintf("select relpages from pg_class where relname = '%s'", table)).Scan(&pages); err != nil {
		return 0, err
	}
	return pages, nil
}

func CreateDB(u DBURL, n DBURL) (DBURL, error) {
	if err := u.Exec("create database " + n.DB); err != nil {
		if pge, ok := err.(*pgconn.PgError); !ok || pge.Code != "42P04" {
			return DBURL{}, err
		}
	}
	return n, nil
}
