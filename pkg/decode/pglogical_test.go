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

type change struct {
	New []interface{}
	Old []interface{}
}

func (c *change) Apply(ctx context.Context, conn *pgx.Conn) (err error) {
	if c.Old == nil {
		_, err = conn.Exec(ctx, "insert into t values ($1,$2,$3,$4,$5,$6)", c.New...)
	} else if c.New != nil {
		_, err = conn.Exec(ctx, "update t set id=$1,uid=$2,txt=$3,bs=$4,js=$5,ts=$6 where id=$7", append(c.New, c.Old...)...)
	} else {
		_, err = conn.Exec(ctx, "delete from t where id=$1", c.Old[0])
	}
	conn.ConnInfo()
	return err
}

func (c *change) Proto() *pb.Change {
	ci := pgtype.NewConnInfo()
	m := &pb.Change{Namespace: "public", Table: "t"}
	if c.New != nil {
		m.NewTuple = []*pb.Field{
			{Name: "id", Oid: 20},
			{Name: "uid", Oid: 2950},
			{Name: "txt", Oid: 25},
			{Name: "bs", Oid: 17},
			{Name: "js", Oid: 3802},
			{Name: "ts", Oid: 1184},
		}
		for i, f := range c.New {
			m.NewTuple[i].Datum, _ = f.(pgtype.BinaryEncoder).EncodeBinary(ci, nil)
		}
		m.Op = pb.Change_INSERT
	}
	if c.Old != nil {
		if c.New == nil || c.New[0].(pgtype.Int8).Int != c.Old[0].(pgtype.Int8).Int {
			m.OldTuple = []*pb.Field{
				{Name: "id", Oid: 20},
			}
			for i, f := range c.Old {
				m.OldTuple[i].Datum, _ = f.(pgtype.BinaryEncoder).EncodeBinary(ci, nil)
			}
		}
		m.Op = pb.Change_UPDATE
	}
	if c.New == nil {
		m.Op = pb.Change_DELETE
	}
	return m
}

const TestSlot = "test_slot"

func TestPGLogicalDecoder(t *testing.T) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://postgres@127.0.0.1/postgres?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	conn.Exec(ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public; create extension if not exists \"uuid-ossp\";")
	conn.Exec(ctx, "create table t (id bigint primary key, uid uuid, txt text, bs bytea, js jsonb, ts timestamptz)")
	conn.Exec(ctx, fmt.Sprintf("select pg_drop_replication_slot('%s')", TestSlot))
	conn.Exec(ctx, sql.CreateLogicalSlot, TestSlot, OutputPlugin)

	changes := []change{
		{
			New: []interface{}{
				pgInt8(1),
				pgUUID("08d6af78-550c-4071-80be-2fece2db0474"),
				pgText(genT(5)),
				pgBytea(genB(5)),
				pgJSON(`{"a": {"b": {"c": {"d": null}}}}`),
				pgTz(time.Now()),
			},
		},
		{
			New: []interface{}{
				pgInt8(2),
				pgUUID("3e89ee8c-3657-4103-99a7-680292a0c22c"),
				pgText(genT(5)),
				pgBytea(genB(5)),
				pgJSON(`{"a": {"b": {"c": {"d": null, "e": 123, "f": "fffffff"}}}}`),
				pgTz(time.Now()),
			},
		},
		{
			New: []interface{}{
				pgInt8(1),
				pgUUID("782b2492-3e7c-431b-9238-c1136ea57190"),
				pgtype.Text{Status: pgtype.Null},
				pgtype.Bytea{Status: pgtype.Null},
				pgJSON(`{"a": {"b": {"c": {"d": null}}}}`),
				pgTz(time.Now()),
			},
			Old: []interface{}{pgInt8(1)},
		},
		{
			New: []interface{}{
				pgInt8(3),
				pgUUID("f0d3ad8e-709f-4f67-9860-e149c671d82a"),
				pgText(genT(5)),
				pgBytea(genB(5)),
				pgJSON(`{"a": {"b": {"c": {"d": null}}}}`),
				pgTz(time.Now()),
			},
			Old: []interface{}{pgInt8(2)},
		},
		{
			Old: []interface{}{pgInt8(3)},
		},
		{
			Old: []interface{}{pgInt8(1)},
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
					if c := m.GetChange(); c == nil || !proto.Equal(c, changes[(count-2)/3].Proto()) {
						t.Fatalf("unexpected %v", m.String())
					}
				}
			}
		default:
			t.Fatal(errors.New("unexpected message"))
		}
	}
}

func pgJSON(t string) pgtype.JSONB {
	return pgtype.JSONB{Bytes: []byte(t), Status: pgtype.Present}
}

func pgText(t string) pgtype.Text {
	return pgtype.Text{String: t, Status: pgtype.Present}
}

func pgBytea(bs []byte) pgtype.Bytea {
	return pgtype.Bytea{Bytes: bs, Status: pgtype.Present}
}

func genT(n int) string {
	builder := strings.Builder{}
	for i := 0; i < n; i++ {
		builder.WriteString("A")
	}
	return builder.String()
}

func genB(n int) []byte {
	builder := bytes.Buffer{}
	for i := 0; i < n; i++ {
		builder.WriteByte('A')
	}
	return builder.Bytes()
}

func pgUUID(t string) pgtype.UUID {
	bs, _ := hex.DecodeString(strings.ReplaceAll(t, "-", ""))
	ret := pgtype.UUID{}
	ret.Set(bs)
	return ret
}

func pgInt8(i int64) pgtype.Int8 {
	return pgtype.Int8{Int: i, Status: pgtype.Present}
}

func pgTz(ts time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: ts, Status: pgtype.Present}
}
