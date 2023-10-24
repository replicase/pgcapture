package decode

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/replicase/pgcapture/internal/test"
	"github.com/replicase/pgcapture/pkg/pb"
	"github.com/replicase/pgcapture/pkg/sql"
	"google.golang.org/protobuf/proto"
)

func TestPGLogicalDecoder(t *testing.T) {
	test.ShouldSkipTestByPGVersion(t, 9.6)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, test.GetPostgresURL())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	conn.Exec(ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public; CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")
	conn.Exec(ctx, "CREATE TABLE t1 (id bigint primary key, uid uuid, txt text, js jsonb, ts timestamptz, bs bytea)")
	conn.Exec(ctx, "CREATE TABLE t2 (id bigint primary key, uid uuid, txt text, js jsonb, ts timestamptz, bs bytea)")
	conn.Exec(ctx, "ALTER TABLE t2 REPLICA IDENTITY FULL")
	conn.Exec(ctx, fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", TestSlot))
	conn.Exec(ctx, sql.CreateLogicalSlot, TestSlot, PGLogicalOutputPlugin)

	schema := NewPGXSchemaLoader(conn)
	if err = schema.RefreshType(); err != nil {
		t.Fatal(err)
	}
	decoder, err := NewPGLogicalDecoder(schema)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	changes := []*change{
		{
			Expect: &pb.Change{Op: pb.Change_INSERT, Schema: "public", Table: "t1",
				New: []*pb.Field{
					{Name: "id", Oid: 20, Value: &pb.Field_Binary{Binary: b(1, pgtype.Int8OID)}},
					{Name: "uid", Oid: 2950, Value: &pb.Field_Binary{Binary: b([]byte("08d6af78-550c-4071-80be-2fece2db0474"), pgtype.UUIDOID)}},
					{Name: "txt", Oid: 25, Value: &pb.Field_Binary{Binary: b(nT(5), pgtype.TextOID)}},
					{Name: "js", Oid: 3802, Value: &pb.Field_Binary{Binary: b([]byte(`{"a": {"b": {"c": {"d": null}}}}`), pgtype.JSONBOID)}},
					{Name: "ts", Oid: 1184, Value: &pb.Field_Binary{Binary: b(now, pgtype.TimestamptzOID)}},
					{Name: "bs", Oid: 17, Value: &pb.Field_Binary{Binary: b(nB(500000), pgtype.ByteaOID)}},
				},
			},
		},
		{
			Expect: &pb.Change{Op: pb.Change_INSERT, Schema: "public", Table: "t1",
				New: []*pb.Field{
					{Name: "id", Oid: 20, Value: &pb.Field_Binary{Binary: b(2, pgtype.Int8OID)}},
					{Name: "uid", Oid: 2950, Value: &pb.Field_Binary{Binary: b([]byte("3e89ee8c-3657-4103-99a7-680292a0c22c"), pgtype.UUIDOID)}},
					{Name: "txt", Oid: 25, Value: &pb.Field_Binary{Binary: b(nT(5), pgtype.TextOID)}},
					{Name: "js", Oid: 3802, Value: &pb.Field_Binary{Binary: b([]byte(`{"a": {"b": {"c": {"d": null, "e": 123, "f": "fffffff"}}}}`), pgtype.JSONBOID)}},
					{Name: "ts", Oid: 1184, Value: &pb.Field_Binary{Binary: b(now, pgtype.TimestamptzOID)}},
					{Name: "bs", Oid: 17, Value: &pb.Field_Binary{Binary: b(nB(500000), pgtype.ByteaOID)}},
				},
			},
		},
		{
			Expect: &pb.Change{Op: pb.Change_UPDATE, Schema: "public", Table: "t1",
				New: []*pb.Field{
					{Name: "id", Oid: 20, Value: &pb.Field_Binary{Binary: b(1, pgtype.Int8OID)}},
					{Name: "uid", Oid: 2950, Value: &pb.Field_Binary{Binary: b([]byte("782b2492-3e7c-431b-9238-c1136ea57190"), pgtype.UUIDOID)}},
					{Name: "txt", Oid: 25, Value: nil},
					{Name: "js", Oid: 3802, Value: &pb.Field_Binary{Binary: b([]byte(`{"a": {"b": {"c": {"d": null}}}}`), pgtype.JSONBOID)}},
					{Name: "ts", Oid: 1184, Value: &pb.Field_Binary{Binary: b(now.Add(time.Second), pgtype.TimestamptzOID)}},
				},
			},
		},
		{
			Expect: &pb.Change{Op: pb.Change_UPDATE, Schema: "public", Table: "t1",
				New: []*pb.Field{
					{Name: "id", Oid: 20, Value: &pb.Field_Binary{Binary: b(3, pgtype.Int8OID)}},
					{Name: "uid", Oid: 2950, Value: &pb.Field_Binary{Binary: b([]byte("f0d3ad8e-709f-4f67-9860-e149c671d82a"), pgtype.UUIDOID)}},
					{Name: "txt", Oid: 25, Value: &pb.Field_Binary{Binary: b(nT(5), pgtype.TextOID)}},
					{Name: "js", Oid: 3802, Value: &pb.Field_Binary{Binary: b([]byte(`{"a": {"b": {"c": {"d": null}}}}`), pgtype.JSONBOID)}},
					{Name: "ts", Oid: 1184, Value: &pb.Field_Binary{Binary: b(now.Add(time.Second), pgtype.TimestamptzOID)}},
				},
				Old: []*pb.Field{
					{Name: "id", Oid: 20, Value: &pb.Field_Binary{Binary: b(2, pgtype.Int8OID)}},
				},
			},
		},
		{
			Expect: &pb.Change{Op: pb.Change_DELETE, Schema: "public", Table: "t1",
				Old: []*pb.Field{
					{Name: "id", Oid: 20, Value: &pb.Field_Binary{Binary: b(3, pgtype.Int8OID)}},
				},
			},
		},
		{
			Expect: &pb.Change{Op: pb.Change_DELETE, Schema: "public", Table: "t1",
				Old: []*pb.Field{
					{Name: "id", Oid: 20, Value: &pb.Field_Binary{Binary: b(1, pgtype.Int8OID)}},
				},
			},
		},
		{
			Expect: &pb.Change{Op: pb.Change_INSERT, Schema: "public", Table: "t2",
				New: []*pb.Field{
					{Name: "id", Oid: 20, Value: &pb.Field_Binary{Binary: b(1, pgtype.Int8OID)}},
					{Name: "uid", Oid: 2950, Value: &pb.Field_Binary{Binary: b([]byte("08d6af78-550c-4071-80be-2fece2db0474"), pgtype.UUIDOID)}},
					{Name: "txt", Oid: 25, Value: &pb.Field_Binary{Binary: b(nT(5), pgtype.TextOID)}},
					{Name: "js", Oid: 3802, Value: &pb.Field_Binary{Binary: b([]byte(`{"a": {"b": {"c": {"d": null}}}}`), pgtype.JSONBOID)}},
					{Name: "ts", Oid: 1184, Value: &pb.Field_Binary{Binary: b(now, pgtype.TimestamptzOID)}},
					{Name: "bs", Oid: 17, Value: &pb.Field_Binary{Binary: b(nB(10), pgtype.ByteaOID)}},
				},
			},
		},
		{
			Expect: &pb.Change{Op: pb.Change_UPDATE, Schema: "public", Table: "t2",
				New: []*pb.Field{
					{Name: "id", Oid: 20, Value: &pb.Field_Binary{Binary: b(1, pgtype.Int8OID)}},
					{Name: "uid", Oid: 2950, Value: &pb.Field_Binary{Binary: b([]byte("782b2492-3e7c-431b-9238-c1136ea57190"), pgtype.UUIDOID)}},
					{Name: "txt", Oid: 25, Value: nil},
					{Name: "js", Oid: 3802, Value: &pb.Field_Binary{Binary: b([]byte(`{"a": {"b": {"c": {"d": null}}}}`), pgtype.JSONBOID)}},
					{Name: "ts", Oid: 1184, Value: &pb.Field_Binary{Binary: b(now.Add(time.Second), pgtype.TimestamptzOID)}},
					{Name: "bs", Oid: 17, Value: &pb.Field_Binary{Binary: b(nB(10), pgtype.ByteaOID)}},
				},
				Old: []*pb.Field{
					{Name: "id", Oid: 20, Value: &pb.Field_Binary{Binary: b(1, pgtype.Int8OID)}},
					{Name: "uid", Oid: 2950, Value: &pb.Field_Binary{Binary: b([]byte("08d6af78-550c-4071-80be-2fece2db0474"), pgtype.UUIDOID)}},
					{Name: "txt", Oid: 25, Value: &pb.Field_Binary{Binary: b(nT(5), pgtype.TextOID)}},
					{Name: "js", Oid: 3802, Value: &pb.Field_Binary{Binary: b([]byte(`{"a": {"b": {"c": {"d": null}}}}`), pgtype.JSONBOID)}},
					{Name: "ts", Oid: 1184, Value: &pb.Field_Binary{Binary: b(now, pgtype.TimestamptzOID)}},
					{Name: "bs", Oid: 17, Value: &pb.Field_Binary{Binary: b(nB(10), pgtype.ByteaOID)}},
				},
			},
		},
		{
			Expect: &pb.Change{Op: pb.Change_DELETE, Schema: "public", Table: "t2",
				Old: []*pb.Field{
					{Name: "id", Oid: 20, Value: &pb.Field_Binary{Binary: b(1, pgtype.Int8OID)}},
					{Name: "uid", Oid: 2950, Value: &pb.Field_Binary{Binary: b([]byte("782b2492-3e7c-431b-9238-c1136ea57190"), pgtype.UUIDOID)}},
					{Name: "js", Oid: 3802, Value: &pb.Field_Binary{Binary: b([]byte(`{"a": {"b": {"c": {"d": null}}}}`), pgtype.JSONBOID)}},
					{Name: "ts", Oid: 1184, Value: &pb.Field_Binary{Binary: b(now.Add(time.Second), pgtype.TimestamptzOID)}},
					{Name: "bs", Oid: 17, Value: &pb.Field_Binary{Binary: b(nB(10), pgtype.ByteaOID)}},
				},
			},
		},
	}
	for _, change := range changes {
		if err = change.Apply(ctx, conn); err != nil {
			t.Fatal(err)
		}
	}

	repl, err := pgconn.Connect(ctx, test.GetPostgresReplURL())
	if err != nil {
		t.Fatal(err)
	}
	defer repl.Close(ctx)

	if err = pglogrepl.StartReplication(ctx, repl, TestSlot, 0, pglogrepl.StartReplicationOptions{PluginArgs: decoder.GetPluginArgs()}); err != nil {
		t.Fatal(err)
	}

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
						fmt.Println(count)
						t.Fatalf("unexpected %v\n %v", m.String(), changes[(count-2)/3].Expect.String())
					}
				}
			}
		default:
			t.Fatal(errors.New("unexpected message"))
		}
	}
}
