package dblog

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/replicase/pgcapture/internal/test"
	"github.com/replicase/pgcapture/pkg/pb"
	"github.com/replicase/pgcapture/pkg/sql"
)

func TestPGXSourceDumper(t *testing.T) {
	ctx := context.Background()
	postgresURL := test.GetPostgresURL()
	conn, err := pgx.Connect(ctx, postgresURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

    // We explicitly use a schema name that requires quoting, and a table name
    // that also requires escaping to check that the dumper code properly
    // handle both of those.
	conn.Exec(ctx, `DROP SCHEMA IF EXISTS "Public" CASCADE; CREATE SCHEMA "Public"`)
	conn.Exec(ctx, "DROP EXTENSION IF EXISTS pgcapture")
	conn.Exec(ctx, sql.InstallExtension)
	conn.Exec(ctx, `CREATE TABLE "Public"."T""1" AS SELECT * FROM generate_series(1,100000) AS id; ANALYZE "Public"."T""1"`)

	dumper, err := NewPGXSourceDumper(ctx, postgresURL)
	if err != nil {
		t.Fatal(err)
	}
	defer dumper.Stop()

	if _, err := dumper.LoadDump(0, &pb.DumpInfoResponse{}); !errors.Is(err, ErrMissingTable) {
		t.Fatal(err)
	}
	if _, err := dumper.LoadDump(0, &pb.DumpInfoResponse{Schema: "Public", Table: `T"1`}); !errors.Is(err, ErrLSNMissing) {
		t.Fatal(err)
	}

	conn.Exec(ctx, "INSERT INTO pgcapture.sources (id,commit) VALUES ($1,$2)", `Public.T"1`, pglogrepl.LSN(0).String())

	if _, err := dumper.LoadDump(0, &pb.DumpInfoResponse{Schema: "any", Table: "any"}); !errors.Is(err, ErrMissingTable) {
		t.Fatal(err)
	}
	if _, err := dumper.LoadDump(0, &pb.DumpInfoResponse{Schema: "Public", Table: "any"}); !errors.Is(err, ErrMissingTable) {
		t.Fatal(err)
	}

	if _, err := dumper.LoadDump(100, &pb.DumpInfoResponse{Schema: "Public", Table: `T"1`}); !errors.Is(err, ErrLSNFallBehind) {
		t.Fatal(err)
	}

	conn.Exec(ctx, "UPDATE pgcapture.sources SET commit=$2 WHERE id = $1", `Public.T"1`, pglogrepl.LSN(100).String())

	var pages int
    if err := conn.QueryRow(ctx, `SELECT relpages FROM pg_class WHERE relname = 'T"1' AND relnamespace::regnamespace::text = '"Public"'`).Scan(&pages); err != nil || pages == 0 {
		t.Fatal(err)
	}

	seq := int32(1)
	for i := uint32(0); i < uint32(pages); i += 5 {
		changes, err := dumper.LoadDump(100, &pb.DumpInfoResponse{Schema: "Public", Table: `T"1`, PageBegin: i, PageEnd: i + 4})
		if err != nil {
			t.Fatal(err)
		}
		for _, change := range changes {
			if change.Schema != "Public" {
				t.Fatal("unexpected")
			}
			if change.Table != `T"1` {
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
