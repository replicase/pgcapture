package dblog

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/rueian/pgcapture/internal/test"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/sql"
)

func TestPGXSourceDumper(t *testing.T) {
	ctx := context.Background()
	postgresURL := test.GetPostgresURL()
	conn, err := pgx.Connect(ctx, postgresURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	conn.Exec(ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public")
	conn.Exec(ctx, "DROP EXTENSION IF EXISTS pgcapture")
	conn.Exec(ctx, sql.InstallExtension)
	conn.Exec(ctx, "CREATE TABLE t1 AS SELECT * FROM generate_series(1,100000) AS id; ANALYZE t1")

	dumper, err := NewPGXSourceDumper(ctx, postgresURL)
	if err != nil {
		t.Fatal(err)
	}
	defer dumper.Stop()

	if _, err := dumper.LoadDump(0, &pb.DumpInfoResponse{}); !errors.Is(err, ErrMissingTable) {
		t.Fatal(err)
	}
	if _, err := dumper.LoadDump(0, &pb.DumpInfoResponse{Schema: "public", Table: "t1"}); !errors.Is(err, ErrLSNMissing) {
		t.Fatal(err)
	}

	conn.Exec(ctx, "INSERT INTO pgcapture.sources (id,commit) VALUES ($1,$2)", "t1", pglogrepl.LSN(0).String())

	if _, err := dumper.LoadDump(0, &pb.DumpInfoResponse{Schema: "any", Table: "any"}); !errors.Is(err, ErrMissingTable) {
		t.Fatal(err)
	}
	if _, err := dumper.LoadDump(0, &pb.DumpInfoResponse{Schema: "public", Table: "any"}); !errors.Is(err, ErrMissingTable) {
		t.Fatal(err)
	}

	if _, err := dumper.LoadDump(100, &pb.DumpInfoResponse{Schema: "public", Table: "t1"}); !errors.Is(err, ErrLSNFallBehind) {
		t.Fatal(err)
	}

	conn.Exec(ctx, "UPDATE pgcapture.sources SET commit=$2 WHERE id = $1", "t1", pglogrepl.LSN(100).String())

	var pages int
	if err := conn.QueryRow(ctx, "select relpages from pg_class where relname = 't1'").Scan(&pages); err != nil || pages == 0 {
		t.Fatal(err)
	}

	seq := int32(1)
	for i := uint32(0); i < uint32(pages); i += 5 {
		changes, err := dumper.LoadDump(100, &pb.DumpInfoResponse{Schema: "public", Table: "t1", PageBegin: i, PageEnd: i + 4})
		if err != nil {
			t.Fatal(err)
		}
		for _, change := range changes {
			if change.Schema != "public" {
				t.Fatal("unexpected")
			}
			if change.Table != "t1" {
				t.Fatal("unexpected")
			}
			if change.Op != pb.Change_UPDATE {
				t.Fatal("unexpected")
			}
			if change.Old != nil {
				t.Fatal("unexpected")
			}
			if len(change.New) != 1 {
				t.Fatal("unexpected")
			}
			if change.New[0].Name != "id" {
				t.Fatal("unexpected")
			}
			if change.New[0].Oid != 23 {
				t.Fatal("unexpected")
			}
			var id pgtype.Int4
			if err := id.DecodeBinary(conn.ConnInfo(), change.New[0].GetBinary()); err != nil {
				t.Fatal(err)
			}
			if id.Int != seq {
				t.Fatal("unexpected")
			}
			seq++
		}
	}
}
