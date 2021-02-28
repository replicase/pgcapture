package decode

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/sql"
)

const TestSlot = "test_slot"

func TestPGLogicalDecoder(t *testing.T) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://postgres@127.0.0.1/postgres?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	conn.Exec(ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public; create extension if not exists \"uuid-ossp\";")
	conn.Exec(ctx, "create table t (id bigint primary key, uid uuid, txt text, js jsonb, ts timestamptz, bs bytea)")
	conn.Exec(ctx, fmt.Sprintf("select pg_drop_replication_slot('%s')", TestSlot))
	conn.Exec(ctx, sql.CreateLogicalSlot, TestSlot, OutputPlugin)

	now := time.Now()
	changes := []*change{
		{
			Expect: &pb.Change{Op: pb.Change_INSERT, Namespace: "public", Table: "t",
				NewTuple: []*pb.Field{
					{Name: "id", Oid: 20, Datum: b(Int8(1))},
					{Name: "uid", Oid: 2950, Datum: b(UUID("08d6af78-550c-4071-80be-2fece2db0474"))},
					{Name: "txt", Oid: 25, Datum: b(Text(nT(5)))},
					{Name: "js", Oid: 3802, Datum: b(JSON(`{"a": {"b": {"c": {"d": null}}}}`))},
					{Name: "ts", Oid: 1184, Datum: b(Tstz(now))},
					{Name: "bs", Oid: 17, Datum: b(Bytea(nB(500000)))},
				},
			},
		},
		{
			Expect: &pb.Change{Op: pb.Change_INSERT, Namespace: "public", Table: "t",
				NewTuple: []*pb.Field{
					{Name: "id", Oid: 20, Datum: b(Int8(2))},
					{Name: "uid", Oid: 2950, Datum: b(UUID("3e89ee8c-3657-4103-99a7-680292a0c22c"))},
					{Name: "txt", Oid: 25, Datum: b(Text(nT(5)))},
					{Name: "js", Oid: 3802, Datum: b(JSON(`{"a": {"b": {"c": {"d": null, "e": 123, "f": "fffffff"}}}}`))},
					{Name: "ts", Oid: 1184, Datum: b(Tstz(now))},
					{Name: "bs", Oid: 17, Datum: b(Bytea(nB(500000)))},
				},
			},
		},
		{
			Expect: &pb.Change{Op: pb.Change_UPDATE, Namespace: "public", Table: "t",
				NewTuple: []*pb.Field{
					{Name: "id", Oid: 20, Datum: b(Int8(1))},
					{Name: "uid", Oid: 2950, Datum: b(UUID("782b2492-3e7c-431b-9238-c1136ea57190"))},
					{Name: "txt", Oid: 25, Datum: b(pgtype.Text{Status: pgtype.Null})},
					{Name: "js", Oid: 3802, Datum: b(JSON(`{"a": {"b": {"c": {"d": null}}}}`))},
					{Name: "ts", Oid: 1184, Datum: b(Tstz(now.Add(time.Second)))},
				},
			},
		},
		{
			Expect: &pb.Change{Op: pb.Change_UPDATE, Namespace: "public", Table: "t",
				NewTuple: []*pb.Field{
					{Name: "id", Oid: 20, Datum: b(Int8(3))},
					{Name: "uid", Oid: 2950, Datum: b(UUID("f0d3ad8e-709f-4f67-9860-e149c671d82a"))},
					{Name: "txt", Oid: 25, Datum: b(Text(nT(5)))},
					{Name: "js", Oid: 3802, Datum: b(JSON(`{"a": {"b": {"c": {"d": null}}}}`))},
					{Name: "ts", Oid: 1184, Datum: b(Tstz(now.Add(time.Second)))},
				},
				OldTuple: []*pb.Field{
					{Name: "id", Oid: 20, Datum: b(Int8(2))},
				},
			},
		},
		{
			Expect: &pb.Change{Op: pb.Change_DELETE, Namespace: "public", Table: "t",
				OldTuple: []*pb.Field{
					{Name: "id", Oid: 20, Datum: b(Int8(3))},
				},
			},
		},
		{
			Expect: &pb.Change{Op: pb.Change_DELETE, Namespace: "public", Table: "t",
				OldTuple: []*pb.Field{
					{Name: "id", Oid: 20, Datum: b(Int8(1))},
				},
			},
		},
	}
	for _, change := range changes {
		if err = change.Apply(ctx, conn); err != nil {
			t.Fatal(err)
		}
	}

	repl, err := pgconn.Connect(ctx, "postgres://postgres@127.0.0.1/postgres?replication=database")
	if err != nil {
		t.Fatal(err)
	}
	defer repl.Close(ctx)

	if err = pglogrepl.StartReplication(ctx, repl, TestSlot, 0, pglogrepl.StartReplicationOptions{PluginArgs: PGLogicalParam}); err != nil {
		t.Fatal(err)
	}

	schema := NewPGXSchemaLoader(conn)
	if err = schema.RefreshType(); err != nil {
		t.Fatal(err)
	}
	decoder := NewPGLogicalDecoder(schema)

	count := 0
recv:
	for {
		ctx, cancel := context.WithTimeout(ctx, time.Second*5)
		msg, err := repl.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		switch msg := msg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					t.Fatal(err)
				}
				m, err := decoder.Decode(xld.WALData)
				if err != nil {
					t.Fatal(err)
				}
				if m == nil {
					continue
				}
				count++

				switch count % 3 {
				case 0:
					if c := m.GetCommit(); c == nil || c.EndLsn != uint64(xld.WALStart) {
						t.Fatalf("unexpected %v %v", m.String(), uint64(xld.WALStart))
					}
					if count == len(changes)*3 {
						break recv
					}
				case 1:
					if b := m.GetBegin(); b == nil || b.FinalLsn < uint64(xld.WALStart) {
						t.Fatalf("unexpected %v", m.String())
					}
				case 2:
					if c := m.GetChange(); c == nil || !proto.Equal(c, changes[(count-2)/3].Expect) {
						t.Fatalf("unexpected %v", m.String())
					}
				}
			}
		default:
			t.Fatal(errors.New("unexpected message"))
		}
	}
}

type change struct {
	Expect *pb.Change
}

func (c *change) Apply(ctx context.Context, conn *pgx.Conn) (err error) {
	vals := make([][]byte, 6)
	fmts := make([]int16, 6)
	oids := make([]uint32, 6)

	for i, t := range c.Expect.NewTuple {
		vals[i] = t.Datum
		oids[i] = t.Oid
		fmts[i] = 1
	}

	switch c.Expect.Op {
	case pb.Change_INSERT:
		_, err = conn.PgConn().ExecParams(ctx, "insert into t values ($1,$2,$3,$4,$5,$6)", vals, oids, fmts, fmts).Close()
	case pb.Change_UPDATE:
		if c.Expect.OldTuple != nil {
			vals[5] = c.Expect.OldTuple[0].Datum
			oids[5] = c.Expect.OldTuple[0].Oid
		} else {
			vals[5] = c.Expect.NewTuple[0].Datum
			oids[5] = c.Expect.NewTuple[0].Oid
		}
		fmts[5] = 1
		_, err = conn.PgConn().ExecParams(ctx, "update t set id=$1,uid=$2,txt=$3,js=$4,ts=$5 where id=$6", vals, oids, fmts, fmts).Close()
	case pb.Change_DELETE:
		vals[0] = c.Expect.OldTuple[0].Datum
		oids[0] = c.Expect.OldTuple[0].Oid
		fmts[0] = 1
		_, err = conn.PgConn().ExecParams(ctx, "delete from t where id=$1", vals[:1], oids[:1], fmts[:1], fmts[:1]).Close()
	}
	return err
}

func JSON(t string) pgtype.JSONB {
	return pgtype.JSONB{Bytes: []byte(t), Status: pgtype.Present}
}

func Text(t string) pgtype.Text {
	return pgtype.Text{String: t, Status: pgtype.Present}
}

func Bytea(bs []byte) pgtype.Bytea {
	return pgtype.Bytea{Bytes: bs, Status: pgtype.Present}
}

func UUID(t string) pgtype.UUID {
	bs, _ := hex.DecodeString(strings.ReplaceAll(t, "-", ""))
	ret := pgtype.UUID{}
	ret.Set(bs)
	return ret
}

func Int8(i int64) pgtype.Int8 {
	return pgtype.Int8{Int: i, Status: pgtype.Present}
}

func Tstz(ts time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: ts, Status: pgtype.Present}
}

func nB(n int) []byte {
	builder := bytes.Buffer{}
	for i := 0; i < n; i++ {
		builder.WriteByte('A')
	}
	return builder.Bytes()
}

func nT(n int) string {
	builder := strings.Builder{}
	for i := 0; i < n; i++ {
		builder.WriteString("A")
	}
	return builder.String()
}

func b(in pgtype.BinaryEncoder) []byte {
	ci := pgtype.NewConnInfo()
	bs, err := in.EncodeBinary(ci, nil)
	if err != nil {
		panic(err)
	}
	return bs
}
