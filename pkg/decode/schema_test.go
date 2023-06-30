package decode

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v4"
)

func TestSchemaLoader(t *testing.T) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://postgres@127.0.0.1/postgres?sslmode=disable")
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

	t.Run("GetTableKey", func(t *testing.T) {
		tables := []struct {
			Primary []string
			Uniques []string
		}{
			{
				Primary: []string{"a"},
			},
			{
				Uniques: []string{"a", "b"},
			},
			{
				Primary: []string{"a", "b"},
				Uniques: []string{"c", "d"},
			},
			{
				// empty
			},
		}

		for i, table := range tables {
			var columns []string
			for _, c := range table.Primary {
				columns = append(columns, c+" int")
			}
			for _, c := range table.Uniques {
				columns = append(columns, c+" int")
			}
			for j := 0; j < 3; j++ {
				columns = append(columns, fmt.Sprintf("c%d int", j))
			}
			if _, err = conn.Exec(ctx, fmt.Sprintf("create table t%d (%s)", i, strings.Join(columns, ","))); err != nil {
				t.Fatal(err)
			}
			if len(table.Primary) > 0 {
				if _, err = conn.Exec(ctx, fmt.Sprintf("alter table t%d add constraint t%dp primary key (%s)", i, i, strings.Join(table.Primary, ","))); err != nil {
					t.Fatal(err)
				}
			}
			if len(table.Uniques) > 0 {
				if _, err = conn.Exec(ctx, fmt.Sprintf("alter table t%d add constraint t%du unique (%s)", i, i, strings.Join(table.Uniques, ","))); err != nil {
					t.Fatal(err)
				}
			}
		}

		if err = schema.RefreshColumnInfo(); err != nil {
			t.Fatalf("RefreshColumnInfo fail: %v", err)
		}
		for i, table := range tables {
			keys, err := schema.GetTableKey("public", "t"+strconv.Itoa(i))
			if len(table.Primary) > 0 {
				if !match(keys, table.Primary) {
					t.Fatalf("GetTableKey not match on %s %v %v", "t"+strconv.Itoa(i), keys, table.Primary)
				}
			} else if len(table.Uniques) > 0 {
				if !match(keys, table.Uniques) {
					t.Fatalf("GetTableKey not match on %s %v %v", "t"+strconv.Itoa(i), keys, table.Uniques)
				}
			} else if !errors.Is(err, ErrSchemaIdentityMissing) {
				t.Fatal(err)
			}
		}
		if _, err = schema.GetTableKey("other", "other"); !errors.Is(err, ErrSchemaIdentityMissing) {
			t.Fatalf("unexpected %v", err)
		}
	})
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
