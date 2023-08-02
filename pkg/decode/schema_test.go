package decode

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/rueian/pgcapture/internal/test"
	"github.com/rueian/pgcapture/pkg/sql"
)

func TestSchemaLoader(t *testing.T) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, test.GetPostgresURL())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	conn.Exec(ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public;")

	schema := NewPGXSchemaLoader(conn)

	t.Run("GetTypeOID", func(t *testing.T) {
		var columns []string
		for _, name := range pgTypeNames {
			if strings.HasPrefix("name", "_") {
				columns = append(columns, name+" "+strings.TrimPrefix(name, "_")+"[]")
			} else {
				columns = append(columns, name+" "+name)
			}
		}

		if _, err = conn.Exec(ctx, fmt.Sprintf("create table t (%s)", strings.Join(columns, ","))); err != nil {
			t.Fatal(err)
		}

		if err = schema.RefreshType(); err != nil {
			t.Fatalf("RefreshType fail: %v", err)
		}
		for _, name := range pgTypeNames {
			dt, ok := conn.ConnInfo().DataTypeForName(name)
			if !ok {
				t.Fatalf("%s type missing from pgx", name)
			}
			if oid, err := schema.GetTypeOID("public", "t", name); err != nil {
				t.Fatalf("GetTypeOID fail: %v", err)
			} else if oid != dt.OID {
				t.Fatalf("GetTypeOID OID mismatch: %s %v %v", name, oid, dt.OID)
			}
		}

		if _, err = schema.GetTypeOID("other", "other", "other"); !errors.Is(err, ErrSchemaTableMissing) {
			t.Fatalf("unexpected %v", err)
		}
		if _, err = schema.GetTypeOID("public", "other", "other"); !errors.Is(err, ErrSchemaTableMissing) {
			t.Fatalf("unexpected %v", err)
		}
		if _, err = schema.GetTypeOID("public", "t", "other"); !errors.Is(err, ErrSchemaColumnMissing) {
			t.Fatalf("unexpected %v", err)
		}
	})

	t.Run("GetColumnInfo", func(t *testing.T) {
		var sv string
		if err = conn.QueryRow(ctx, sql.ServerVersionNum).Scan(&sv); err != nil {
			t.Fatal(err)
		}

		pgVersion, err := strconv.ParseInt(sv, 10, 64)
		if err != nil {
			t.Fatal(err)
		}

		tables := []tableFixture{
			{
				// empty
			},
			{
				primary: []string{"a"},
			},
			{
				uniques: []string{"a", "b"},
			},
			{
				primary: []string{"a", "b"},
				uniques: []string{"c", "d"},
			},
			{
				primary:             []string{"a"},
				identityGenerations: []string{"a"},
			},
			{
				primary:   []string{"a"},
				generated: []string{"b", "c"},
				// since the generated column supported by pg12 or above
				minVer: 120000,
			},
		}

		for i, table := range tables {
			if pgVersion < table.minVer {
				fmt.Printf("skip table t%d due to pg version %d\n", i, pgVersion)
				continue
			}

			if err = table.create(ctx, conn, i); err != nil {
				t.Fatal(err)
			}
		}

		if err = schema.RefreshColumnInfo(); err != nil {
			t.Fatalf("RefreshColumnInfo fail: %v", err)
		}

		for i, table := range tables {
			if pgVersion < table.minVer {
				continue
			}

			info, err := schema.GetColumnInfo("public", "t"+strconv.Itoa(i))
			if len(table.primary) == 0 && len(table.uniques) == 0 && len(table.identityGenerations) == 0 {
				// the empty table: should get the schema identity missing error
				if !errors.Is(err, ErrSchemaIdentityMissing) {
					t.Fatalf("unexpected %v", err)
				}
				continue
			}

			keys := info.ListKeys()
			if len(table.primary) > 0 {
				if !match(keys, table.primary) {
					t.Fatalf("GetTableKey not match on %s %v %v", "t"+strconv.Itoa(i), keys, table.primary)
				}
			} else if len(table.uniques) > 0 {
				if !match(keys, table.uniques) {
					t.Fatalf("GetTableKey not match on %s %v %v", "t"+strconv.Itoa(i), keys, table.uniques)
				}
			}

			for _, c := range table.identityGenerations {
				if !info.IsIdentityGeneration(c) {
					t.Fatalf("The column: %s should be identity generated", c)
				}
			}

			for _, c := range table.generated {
				if !info.IsGenerated(c) {
					t.Fatalf("The column: %s should be generated", c)
				}
			}
		}
		if _, err = schema.GetTableKey("other", "other"); !errors.Is(err, ErrSchemaIdentityMissing) {
			t.Fatalf("unexpected %v", err)
		}
	})

	t.Run("GetVersion", func(t *testing.T) {
		if _, err := schema.GetVersion(); err != nil {
			t.Fatal(err)
		}
	})
}

type tableFixture struct {
	primary             []string
	uniques             []string
	identityGenerations []string
	generated           []string
	minVer              int64
}

func (t tableFixture) create(ctx context.Context, conn *pgx.Conn, tag int) (err error) {
	var columns []string
	for _, c := range t.primary {
		columns = append(columns, c+" int")
	}
	for _, c := range t.uniques {
		columns = append(columns, c+" int")
	}
	for j := 0; j < 3; j++ {
		columns = append(columns, fmt.Sprintf("c%d int", j))
	}
	if _, err = conn.Exec(ctx, fmt.Sprintf("create table t%d (%s)", tag, strings.Join(columns, ","))); err != nil {
		return
	}

	if len(t.primary) > 0 {
		q := fmt.Sprintf("alter table t%d add constraint t%dp primary key (%s)", tag, tag, strings.Join(t.primary, ","))
		if _, err = conn.Exec(ctx, q); err != nil {
			return
		}
	}

	for _, c := range t.identityGenerations {
		q := fmt.Sprintf("alter table t%d alter column %s add generated always as identity", tag, c)
		if _, err = conn.Exec(ctx, q); err != nil {
			return
		}
	}

	if len(t.uniques) > 0 {
		q := fmt.Sprintf("alter table t%d add constraint t%du unique (%s)", tag, tag, strings.Join(t.uniques, ","))
		if _, err = conn.Exec(ctx, q); err != nil {
			return
		}
	}

	for _, g := range t.generated {
		q := fmt.Sprintf("alter table t%d add column %s int generated always as (c0 * 2) stored", tag, g)
		if _, err = conn.Exec(ctx, q); err != nil {
			fmt.Println("failed to exec alter table", q, "\n", err)
			return
		}
	}

	return
}

var pgTypeNames = []string{
	"_aclitem",
	"_bool",
	"_bpchar",
	"_bytea",
	"_cidr",
	"_date",
	"_float4",
	"_float8",
	"_inet",
	"_int2",
	"_int4",
	"_int8",
	"_numeric",
	"_text",
	"_timestamp",
	"_timestamptz",
	"_uuid",
	"_varchar",
	"aclitem",
	"bit",
	"bool",
	"box",
	"bpchar",
	"bytea",
	"cid",
	"cidr",
	"circle",
	"date",
	"daterange",
	"float4",
	"float8",
	"inet",
	"int2",
	"int4",
	"int4range",
	"int8",
	"int8range",
	"interval",
	"json",
	"jsonb",
	"line",
	"lseg",
	"macaddr",
	"name",
	"numeric",
	"numrange",
	"oid",
	"path",
	"point",
	"polygon",
	"text",
	"tid",
	"time",
	"timestamp",
	"timestamptz",
	"tsrange",
	"_tsrange",
	"tstzrange",
	"_tstzrange",
	"uuid",
	"varbit",
	"varchar",
	"xid",
}

func match(s1, s2 []string) bool {
	return contains(s1, s2) && contains(s2, s1)
}

func contains(s1, s2 []string) bool {
next:
	for _, a := range s1 {
		for _, b := range s2 {
			if a == b {
				continue next
			}
		}
		return false
	}
	return true
}
