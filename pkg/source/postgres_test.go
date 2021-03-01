package source

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v4"
	"github.com/rueian/pgcapture/pkg/decode"
	"github.com/rueian/pgcapture/pkg/pb"
	"google.golang.org/protobuf/proto"
)

const TestSlot = "test_slot"

func TestPGXSource_Capture(t *testing.T) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://postgres@127.0.0.1/postgres?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	conn.Exec(ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
	conn.Exec(ctx, "DROP EXTENSION IF EXISTS pgcapture")
	conn.Exec(ctx, fmt.Sprintf("select pg_drop_replication_slot('%s')", TestSlot))

	newPGXSource := func() *PGXSource {
		return &PGXSource{
			SetupConnStr: "postgres://postgres@127.0.0.1/postgres?sslmode=disable",
			ReplConnStr:  "postgres://postgres@127.0.0.1/postgres?replication=database",
			ReplSlot:     TestSlot,
		}
	}

	src := newPGXSource()
	src.CreateSlot = true

	// test from latest
	changes, err := src.Capture(Checkpoint{})
	if err != nil {
		t.Fatal(err)
	}

	txs := []*TxTest{
		{
			SQL: "create table t1 (id1 bigint)",
			Check: func(tx *TxTest) {
				tx.Tx = readTx(t, changes, 1)
				if change := tx.Tx.Changes[0].Message.GetChange(); !expectedDDL(change, "create table t1 (id1 bigint)") {
					t.Fatalf("unexpected %v", change.String())
				}
			},
		},
		{
			SQL: "insert into t1 values (1)",
			Check: func(tx *TxTest) {
				tx.Tx = readTx(t, changes, 1)
				change := tx.Tx.Changes[0].Message.GetChange()
				expect := &pb.Change{Op: pb.Change_INSERT, Namespace: "public", Table: "t1", NewTuple: []*pb.Field{{Name: "id1", Oid: 20, Datum: []byte{0, 0, 0, 0, 0, 0, 0, 1}}}}
				if !proto.Equal(change, expect) {
					t.Fatalf("unexpected %v", change.String())
				}
			},
		},
		{
			SQL: "create table t2 (id2 text)",
			Check: func(tx *TxTest) {
				tx.Tx = readTx(t, changes, 1)
				if change := tx.Tx.Changes[0].Message.GetChange(); !expectedDDL(change, "create table t2 (id2 text)") {
					t.Fatalf("unexpected %v", change.String())
				}
			},
		},
		{
			SQL: "insert into t2 values ('id2')",
			Check: func(tx *TxTest) {
				tx.Tx = readTx(t, changes, 1)
				change := tx.Tx.Changes[0].Message.GetChange()
				expect := &pb.Change{Op: pb.Change_INSERT, Namespace: "public", Table: "t2", NewTuple: []*pb.Field{{Name: "id2", Oid: 25, Datum: []byte("id2")}}}
				if !proto.Equal(change, expect) {
					t.Fatalf("unexpected %v", change.String())
				}
			},
		},
	}

	go func() {
		for _, tx := range txs {
			if _, err = conn.Exec(ctx, tx.SQL); err != nil {
				t.Fatal(err)
			}
		}
	}()

	// test schema refresh
	for _, tx := range txs {
		tx.Check(tx)
	}
	src.Stop()

	// test restart on un-committed position
	src = newPGXSource()
	changes, err = src.Capture(txs[1].Tx.Commit.Checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	for _, tx := range txs[2:] {
		tx.Check(tx)
	}

	// test commit lsn
	commit := txs[len(txs)-1].Tx.Commit
	src.Commit(commit.Checkpoint)
	src.Stop()

	var lsn string
	if err = conn.QueryRow(ctx, "select confirmed_flush_lsn from pg_replication_slots where slot_name = $1", TestSlot).Scan(&lsn); err != nil {
		t.Fatal(err)
	}

	if lsn != pglogrepl.LSN(commit.Checkpoint.LSN).String() {
		t.Fatalf("unexpected %v", lsn)
	}
}

type TxTest struct {
	SQL   string
	Check func(test *TxTest)
	Tx    Tx
}

type Tx struct {
	Begin   Change
	Commit  Change
	Changes []Change
}

func readTx(t *testing.T, changes chan Change, n int) (tx Tx) {
	if m := <-changes; m.Message.GetBegin() == nil {
		t.Fatalf("unexpected %v", m.Message.String())
	} else {
		tx.Begin = m
	}

	for i := 0; i < n; i++ {
		if m := <-changes; m.Message.GetChange() == nil {
			t.Fatalf("unexpected %v", m.Message.String())
		} else {
			tx.Changes = append(tx.Changes, m)
		}
	}

	if m := <-changes; m.Message.GetCommit() == nil {
		t.Fatalf("unexpected %v", m.Message.String())
	} else {
		tx.Commit = m
	}
	return
}

func expectedDDL(change *pb.Change, sql string) bool {
	return change.Namespace == decode.ExtensionNamespace &&
		change.Table == decode.ExtensionDDLLogs &&
		change.NewTuple[1].Name == "query" &&
		bytes.Equal(change.NewTuple[1].Datum, []byte(sql))
}
