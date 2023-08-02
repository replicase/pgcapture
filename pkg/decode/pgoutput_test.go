package decode

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
	"github.com/rueian/pgcapture/internal/test"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/sql"
	"google.golang.org/protobuf/proto"
)

func TestPGOutputDecoder(t *testing.T) {
	test.ShouldSkipTestByPGVersion(t, 14)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, test.GetPostgresURL())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	conn.Exec(ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public; CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")
	conn.Exec(ctx, "CREATE TABLE t (id bigint primary key, uid uuid, txt text, js jsonb, ts timestamptz, bs bytea)")
	conn.Exec(ctx, fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", TestSlot))
	conn.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES;", TestSlot))
	conn.Exec(ctx, sql.CreateLogicalSlot, TestSlot, PGOutputPlugin)

	schema := NewPGXSchemaLoader(conn)
	if err = schema.RefreshType(); err != nil {
		t.Fatal(err)
	}
	decoder := NewPGOutputDecoder(schema, TestSlot)

	now := time.Now()

	changes := []*change{
		{
			Expect: &pb.Change{Op: pb.Change_INSERT, Schema: "public", Table: "t",
				New: []*pb.Field{
					{Name: "id", Oid: 20, Value: &pb.Field_Binary{Binary: b(Int8(1))}},
					{Name: "uid", Oid: 2950, Value: &pb.Field_Binary{Binary: b(UUID("08d6af78-550c-4071-80be-2fece2db0474"))}},
					{Name: "txt", Oid: 25, Value: &pb.Field_Binary{Binary: b(Text(nT(5)))}},
					{Name: "js", Oid: 3802, Value: &pb.Field_Binary{Binary: b(JSON(`{"a": {"b": {"c": {"d": null}}}}`))}},
					{Name: "ts", Oid: 1184, Value: &pb.Field_Binary{Binary: b(Tstz(now))}},
					{Name: "bs", Oid: 17, Value: &pb.Field_Binary{Binary: b(Bytea(nB(500000)))}},
				},
			},
		},
		{
			Expect: &pb.Change{Op: pb.Change_INSERT, Schema: "public", Table: "t",
				New: []*pb.Field{
					{Name: "id", Oid: 20, Value: &pb.Field_Binary{Binary: b(Int8(2))}},
					{Name: "uid", Oid: 2950, Value: &pb.Field_Binary{Binary: b(UUID("3e89ee8c-3657-4103-99a7-680292a0c22c"))}},
					{Name: "txt", Oid: 25, Value: &pb.Field_Binary{Binary: b(Text(nT(5)))}},
					{Name: "js", Oid: 3802, Value: &pb.Field_Binary{Binary: b(JSON(`{"a": {"b": {"c": {"d": null, "e": 123, "f": "fffffff"}}}}`))}},
					{Name: "ts", Oid: 1184, Value: &pb.Field_Binary{Binary: b(Tstz(now))}},
					{Name: "bs", Oid: 17, Value: &pb.Field_Binary{Binary: b(Bytea(nB(500000)))}},
				},
			},
		},
		{
			Expect: &pb.Change{Op: pb.Change_UPDATE, Schema: "public", Table: "t",
				New: []*pb.Field{
					{Name: "id", Oid: 20, Value: &pb.Field_Binary{Binary: b(Int8(1))}},
					{Name: "uid", Oid: 2950, Value: &pb.Field_Binary{Binary: b(UUID("782b2492-3e7c-431b-9238-c1136ea57190"))}},
					{Name: "txt", Oid: 25, Value: nil},
					{Name: "js", Oid: 3802, Value: &pb.Field_Binary{Binary: b(JSON(`{"a": {"b": {"c": {"d": null}}}}`))}},
					{Name: "ts", Oid: 1184, Value: &pb.Field_Binary{Binary: b(Tstz(now.Add(time.Second)))}},
				},
			},
		},
		{
			Expect: &pb.Change{Op: pb.Change_UPDATE, Schema: "public", Table: "t",
				Old: []*pb.Field{
					{Name: "id", Oid: 20, Value: &pb.Field_Binary{Binary: b(Int8(2))}},
				},
				New: []*pb.Field{
					{Name: "id", Oid: 20, Value: &pb.Field_Binary{Binary: b(Int8(3))}},
					{Name: "uid", Oid: 2950, Value: &pb.Field_Binary{Binary: b(UUID("f0d3ad8e-709f-4f67-9860-e149c671d82a"))}},
					{Name: "txt", Oid: 25, Value: &pb.Field_Binary{Binary: b(Text(nT(6)))}},
					{Name: "js", Oid: 3802, Value: &pb.Field_Binary{Binary: b(JSON(`{"a": {"b": {"c": {"d": null}}}}`))}},
					{Name: "ts", Oid: 1184, Value: &pb.Field_Binary{Binary: b(Tstz(now.Add(time.Second)))}},
				},
			},
		},
		{
			Expect: &pb.Change{Op: pb.Change_DELETE, Schema: "public", Table: "t",
				Old: []*pb.Field{
					{Name: "id", Oid: 20, Value: &pb.Field_Binary{Binary: b(Int8(3))}},
				},
			},
		},
		{
			Expect: &pb.Change{Op: pb.Change_DELETE, Schema: "public", Table: "t",
				Old: []*pb.Field{
					{Name: "id", Oid: 20, Value: &pb.Field_Binary{Binary: b(Int8(1))}},
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
						t.Fatalf("unexpected %v", m.String())
					}
				}
			}
		default:
			t.Fatal(errors.New("unexpected message"))
		}
	}
}
