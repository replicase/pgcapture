package source

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/rueian/pgcapture/internal/test"
	"github.com/rueian/pgcapture/pkg/cursor"
	"github.com/rueian/pgcapture/pkg/decode"
	"github.com/rueian/pgcapture/pkg/pb"
	"google.golang.org/protobuf/proto"
)

const TestSlot = "test_slot"

func newPGXSource(decodePlugin string) *PGXSource {
	return &PGXSource{
		SetupConnStr: test.GetPostgresURL(),
		ReplConnStr:  test.GetPostgresReplURL(),
		ReplSlot:     TestSlot,
		DecodePlugin: decodePlugin,
	}
}

func newPGConn(ctx context.Context) (*pgx.Conn, error) {
	conn, err := pgx.Connect(ctx, test.GetPostgresURL())
	if err != nil {
		return nil, err
	}

	conn.Exec(ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
	conn.Exec(ctx, "DROP EXTENSION IF EXISTS pgcapture")

	return conn, nil
}

type PGXSourceTest struct {
	decodePlugin string
	shouldSkip   func(t *testing.T)
	newPGConn    func(ctx context.Context) (*pgx.Conn, error)
	newPGXSource func() *PGXSource
}

var pgxSourceTests = []PGXSourceTest{
	{
		decodePlugin: decode.PGLogicalOutputPlugin,
		shouldSkip: func(t *testing.T) {
			test.ShouldSkipTestByPGVersion(t, 9.6)
		},
		newPGConn: func(ctx context.Context) (*pgx.Conn, error) {
			conn, err := newPGConn(ctx)
			if err != nil {
				return nil, err
			}
			conn.Exec(ctx, fmt.Sprintf("select pg_drop_replication_slot('%s')", TestSlot))

			return conn, nil
		},
		newPGXSource: func() *PGXSource {
			src := newPGXSource(decode.PGLogicalOutputPlugin)
			src.CreateSlot = true
			return src
		},
	},
	{
		decodePlugin: decode.PGOutputPlugin,
		shouldSkip: func(t *testing.T) {
			test.ShouldSkipTestByPGVersion(t, 14)
		},
		newPGConn: func(ctx context.Context) (*pgx.Conn, error) {
			conn, err := newPGConn(ctx)
			if err != nil {
				return nil, err
			}
			conn.Exec(ctx, fmt.Sprintf("select pg_drop_replication_slot('%s')", TestSlot))
			conn.Exec(ctx, fmt.Sprintf("DROP PUBLICATION %s", TestSlot))

			return conn, nil
		},
		newPGXSource: func() *PGXSource {
			src := newPGXSource(decode.PGOutputPlugin)
			src.CreateSlot = true
			src.CreatePublication = true
			return src
		},
	},
}

func TestPGXSource_Capture(t *testing.T) {
	for _, te := range pgxSourceTests {
		t.Run(te.decodePlugin, func(t *testing.T) {
			te.shouldSkip(t)

			ctx := context.Background()
			src := te.newPGXSource()
			conn, err := te.newPGConn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close(ctx)

			// test from latest
			changes, err := src.Capture(cursor.Checkpoint{})
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
						expect := &pb.Change{Op: pb.Change_INSERT, Schema: "public", Table: "t1", New: []*pb.Field{{Name: "id1", Oid: 20, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 0, 0, 0, 0, 1}}}}}
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
						expect := &pb.Change{Op: pb.Change_INSERT, Schema: "public", Table: "t2", New: []*pb.Field{{Name: "id2", Oid: 25, Value: &pb.Field_Binary{Binary: []byte("id2")}}}}
						if !proto.Equal(change, expect) {
							t.Fatalf("unexpected %v", change.String())
						}
					},
				},
			}

			for _, tx := range txs {
				if _, err := conn.Exec(ctx, tx.SQL); err != nil {
					t.Fatal(err)
				}
			}

			time.Sleep(time.Second)
			// test schema refresh
			for _, tx := range txs {
				tx.Check(tx)
			}
			src.Stop()

			// test restart on FinalLSN of Change position, should start from the beginning of the same tx
			src = newPGXSource(src.DecodePlugin)
			changes, err = src.Capture(txs[1].Tx.Changes[0].Checkpoint)
			if err != nil {
				t.Fatal(err)
			}

			time.Sleep(time.Second)
			for _, tx := range txs[1:] {
				tx.Check(tx)
			}
			src.Stop()

			// test restart on CommitLSN of Commit position, should start from this tx
			src = newPGXSource(src.DecodePlugin)
			changes, err = src.Capture(txs[1].Tx.Commit.Checkpoint)
			if err != nil {
				t.Fatal(err)
			}

			time.Sleep(time.Second)
			for _, tx := range txs[1:] {
				tx.Check(tx)
			}

			// test commit lsn
			commit := txs[len(txs)-1].Tx.Commit
			src.Commit(commit.Checkpoint)
			src.Stop()

			if n := src.TxCounter(); n == 0 {
				t.Fatal("TxCounter should > 0")
			}

			time.Sleep(6 * time.Second)
			var lsn string
			if err = conn.QueryRow(ctx, "select confirmed_flush_lsn from pg_replication_slots where slot_name = $1", TestSlot).Scan(&lsn); err != nil {
				t.Fatal(err)
			}

			if lsn != pglogrepl.LSN(commit.Checkpoint.LSN).String() {
				t.Fatalf("unexpected %v", lsn)
			}
		})
	}
}

func TestPGXSource_DuplicatedCapture(t *testing.T) {
	for _, te := range pgxSourceTests {
		t.Run(te.decodePlugin, func(t *testing.T) {
			te.shouldSkip(t)

			ctx := context.Background()
			conn, err := te.newPGConn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close(ctx)
			src := te.newPGXSource()
			_, err = src.Capture(cursor.Checkpoint{})
			if err != nil {
				t.Fatal(err)
			}
			defer src.Stop()

			// duplicated
			src2 := newPGXSource(src.DecodePlugin)
			if _, err = src2.Capture(cursor.Checkpoint{}); err == nil || !strings.Contains(err.Error(), fmt.Sprintf("replication slot \"%s\" is active", TestSlot)) {
				t.Fatal("duplicated pgx source")
			}
			src2.Stop()
		})
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
	var finalLSN uint64

	if m := <-changes; m.Message.GetBegin() == nil {
		t.Fatalf("unexpected %v", m.Message.String())
	} else {
		tx.Begin = m
		begin := m.Message.GetBegin()
		if m.Checkpoint.LSN != begin.FinalLsn || m.Checkpoint.Seq != 0 {
			t.Fatalf("unexpected begin checkpoint %v", m.Checkpoint)
		}
		finalLSN = begin.FinalLsn
	}

	for i := uint32(1); i <= uint32(n); i++ {
		if m := <-changes; m.Message.GetChange() == nil {
			t.Fatalf("unexpected %v", m.Message.String())
		} else {
			if m.Checkpoint.LSN != finalLSN || m.Checkpoint.Seq != i {
				t.Fatalf("unexpected change checkpoint %v", m.Checkpoint)
			}
			tx.Changes = append(tx.Changes, m)
		}
	}

	if m := <-changes; m.Message.GetCommit() == nil {
		t.Fatalf("unexpected %v", m.Message.String())
	} else {
		commit := m.Message.GetCommit()
		if m.Checkpoint.LSN != commit.CommitLsn || m.Checkpoint.Seq == 0 || commit.CommitLsn != finalLSN {
			t.Fatalf("unexpected commit checkpoint %v", m.Checkpoint)
		}
		tx.Commit = m
	}
	return
}

func expectedDDL(change *pb.Change, sql string) bool {
	return change.Schema == decode.ExtensionSchema &&
		change.Table == decode.ExtensionDDLLogs &&
		change.New[1].Name == "query" &&
		bytes.Equal(change.New[1].GetBinary(), []byte(sql))
}
