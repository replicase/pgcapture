package sink

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/rueian/pgcapture/internal/test"
	"github.com/rueian/pgcapture/pkg/cursor"
	"github.com/rueian/pgcapture/pkg/decode"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/source"
	"github.com/rueian/pgcapture/pkg/sql"
)

func newPGXSink() *PGXSink {
	return &PGXSink{
		ConnStr:  test.GetPostgresURL(),
		SourceID: "repl_test",
		Renice:   -10,
	}
}

func TestPGXSink(t *testing.T) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, test.GetPostgresURL())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	conn.Exec(ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public")
	conn.Exec(ctx, "DROP EXTENSION IF EXISTS pgcapture")

	var sv string
	if err = conn.QueryRow(ctx, sql.ServerVersionNum).Scan(&sv); err != nil {
		t.Fatal(err)
	}

	pgVersion, err := strconv.ParseInt(sv, 10, 64)
	if err != nil {
		t.Fatal(err)
	}

	sink := newPGXSink()

	cp, err := sink.Setup()
	if err != nil {
		t.Fatal(err)
	}

	if sink.ReplicationLagMilliseconds() != -1 {
		t.Fatalf("initial replicaition lag should be -1")
	}

	// test empty checkpoint
	if cp.LSN != 0 || len(cp.Data) != 0 {
		t.Fatalf("checkpoint of empty topic should be zero")
	}

	changes := make(chan source.Change)
	committed := sink.Apply(changes)

	lsn := uint64(0)
	now := time.Now()

	// ignore duplicated start lsn
	changes <- source.Change{
		Checkpoint: cursor.Checkpoint{LSN: lsn},
		Message:    &pb.Message{Type: &pb.Message_Begin{Begin: &pb.Begin{}}},
	}

	type task struct {
		chs    []*pb.Change
		verify func(t *testing.T)
		minVer int64
	}

	doTx := func(opt task) {
		if opt.minVer > pgVersion {
			log.Printf("skip task due to pg version %d < %d", pgVersion, opt.minVer)
			return
		}

		chs := opt.chs
		now = now.Add(time.Second)
		ts := now.Unix()*1000000 + int64(now.Nanosecond())/1000 - microsecFromUnixEpochToY2K
		lsn++
		changes <- source.Change{
			Checkpoint: cursor.Checkpoint{LSN: lsn, Data: []byte(now.Format(time.RFC3339Nano))},
			Message:    &pb.Message{Type: &pb.Message_Begin{Begin: &pb.Begin{}}},
		}
		for _, change := range chs {
			now = now.Add(time.Second)
			lsn++
			changes <- source.Change{
				Checkpoint: cursor.Checkpoint{LSN: lsn, Data: []byte(now.Format(time.RFC3339Nano))},
				Message:    &pb.Message{Type: &pb.Message_Change{Change: change}},
			}
		}
		now = now.Add(time.Second)
		lsn++
		changes <- source.Change{
			Checkpoint: cursor.Checkpoint{LSN: lsn, Data: []byte(now.Format(time.RFC3339Nano))},
			Message:    &pb.Message{Type: &pb.Message_Commit{Commit: &pb.Commit{CommitTime: uint64(ts)}}},
		}
		if cp := <-committed; cp.LSN != lsn || string(cp.Data) != now.Format(time.RFC3339Nano) {
			t.Fatalf("unexpected %v %v %v", cp, lsn, now)
		}
		if err = sink.Error(); err != nil {
			t.Fatalf("unexpected %v", err)
		}
		if sink.ReplicationLagMilliseconds() == -1 {
			t.Fatalf("replicaition lag should not be -1")
		}

		if opt.verify != nil {
			opt.verify(t)
		}
	}

	doTx(task{
		chs: []*pb.Change{{
			Op:     pb.Change_INSERT,
			Schema: decode.ExtensionSchema,
			Table:  decode.ExtensionDDLLogs,
			New: []*pb.Field{
				{Name: "query", Value: &pb.Field_Binary{Binary: []byte(`create table t3 (f1 int, f2 int, f3 text, primary key(f1, f2))`)}},
				{Name: "tags", Value: &pb.Field_Binary{Binary: tags("CREATE TABLE")}},
			},
		}},
	})

	doTx(task{
		chs: []*pb.Change{{
			Op:     pb.Change_INSERT,
			Schema: "public",
			Table:  "t3",
			New: []*pb.Field{
				{Name: "f1", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 1}}},
				{Name: "f2", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 1}}},
				{Name: "f3", Oid: 25, Value: &pb.Field_Binary{Binary: []byte{'A'}}},
			},
		}},
		verify: func(t *testing.T) {
			var f3 string
			err := conn.QueryRow(ctx, "select f3 from t3 where f1 = $1 and f2 = $2", 1, 1).Scan(&f3)
			if err != nil {
				t.Fatal(err)
			}
			if f3 != "A" {
				t.Fatalf("unexpected f3 %v", f3)
			}
		},
	})

	doTx(task{
		chs: []*pb.Change{{
			Op:     pb.Change_UPDATE,
			Schema: "public",
			Table:  "t3",
			New: []*pb.Field{
				{Name: "f1", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 1}}},
				{Name: "f2", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 1}}},
				{Name: "f3", Oid: 25, Value: &pb.Field_Binary{Binary: []byte{'B'}}},
			},
		}},
		verify: func(t *testing.T) {
			var f3 string
			err := conn.QueryRow(ctx, "select f3 from t3 where f1 = $1 and f2 = $2", 1, 1).Scan(&f3)
			if err != nil {
				t.Fatal(err)
			}
			if f3 != "B" {
				t.Fatalf("unexpected f3 %v", f3)
			}
		},
	})

	// update with key changes
	doTx(task{
		chs: []*pb.Change{{
			Op:     pb.Change_UPDATE,
			Schema: "public",
			Table:  "t3",
			New: []*pb.Field{
				{Name: "f1", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 2}}},
				{Name: "f2", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 3}}},
				{Name: "f3", Oid: 25, Value: &pb.Field_Binary{Binary: []byte{'B'}}},
			},
			Old: []*pb.Field{
				{Name: "f1", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 1}}},
				{Name: "f2", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 1}}},
			},
		}},
		verify: func(t *testing.T) {
			var f3 string
			err := conn.QueryRow(ctx, "select f3 from t3 where f1 = $1 and f2 = $2", 2, 3).Scan(&f3)
			if err != nil {
				t.Fatal(err)
			}
			if f3 != "B" {
				t.Fatalf("unexpected f3 %v", f3)
			}

			var count int
			err = conn.QueryRow(ctx, "select count(1) from t3 where f1 = $1 and f2 = $2", 1, 1).Scan(&count)
			if err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatalf("unexpected count %v", count)
			}
		},
	})

	// handle select create case
	doTx(task{
		chs: []*pb.Change{{
			Op:     pb.Change_INSERT,
			Schema: decode.ExtensionSchema,
			Table:  decode.ExtensionDDLLogs,
			New:    []*pb.Field{{Name: "query", Value: &pb.Field_Binary{Binary: []byte(`select * into t4 from t3`)}}, {Name: "tags", Value: &pb.Field_Binary{Binary: tags("SELECT INTO")}}},
		}, { // the data change after select create should be ignored
			Op:     pb.Change_INSERT,
			Schema: "public",
			Table:  "t4",
			New: []*pb.Field{
				{Name: "f1", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 2}}},
				{Name: "f2", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 3}}},
				{Name: "f3", Oid: 25, Value: &pb.Field_Binary{Binary: []byte{'X'}}},
			},
		}},
		verify: func(t *testing.T) {
			var f3 string
			err := conn.QueryRow(ctx, "select f3 from t4 where f1 = $1 and f2 = $2", 2, 3).Scan(&f3)
			if err != nil {
				t.Fatal(err)
			}
			if f3 != "B" {
				// the value should not be updated
				t.Fatalf("unexpected f3 %v", f3)
			}
		},
	})

	doTx(task{
		chs: []*pb.Change{{
			Op:     pb.Change_DELETE,
			Schema: "public",
			Table:  "t3",
			Old: []*pb.Field{
				{Name: "f1", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 2}}},
				{Name: "f2", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 3}}},
			},
		}},
		verify: func(t *testing.T) {
			var count int
			err := conn.QueryRow(ctx, "select count(1) from t3 where f1 = $1 and f2 = $2", 2, 3).Scan(&count)
			if err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatalf("unexpected count %v", count)
			}
		},
	})

	doTx(task{
		chs: []*pb.Change{{
			Op:     pb.Change_INSERT,
			Schema: decode.ExtensionSchema,
			Table:  decode.ExtensionDDLLogs,
			New: []*pb.Field{
				{Name: "query", Value: &pb.Field_Binary{Binary: []byte(`create table t5 (f1 int generated always as identity primary key, f2 int, f3 text)`)}},
				{Name: "tags", Value: &pb.Field_Binary{Binary: tags("CREATE TABLE")}},
			},
		}},
	})

	doTx(task{
		chs: []*pb.Change{{
			Op:     pb.Change_INSERT,
			Schema: "public",
			Table:  "t5",
			New: []*pb.Field{
				{Name: "f1", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 20}}},
				{Name: "f2", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 20}}},
				{Name: "f3", Oid: 25, Value: &pb.Field_Binary{Binary: []byte{'D'}}},
			},
		}},
		verify: func(t *testing.T) {
			var (
				f2 int
				f3 string
			)
			// should override the system value for generated identity column f1
			err := conn.QueryRow(ctx, "select f2, f3 from t5 where f1 = $1", 20).Scan(&f2, &f3)
			if err != nil {
				t.Fatal(err)
			}
			if f2 != 20 || f3 != "D" {
				t.Fatalf("unexpected value for (f2, f3): (%v, %v)", f2, f3)
			}
		},
	})

	doTx(task{
		chs: []*pb.Change{{
			Op:     pb.Change_UPDATE,
			Schema: "public",
			Table:  "t5",
			New: []*pb.Field{
				{Name: "f1", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 20}}},
				{Name: "f2", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 21}}},
				{Name: "f3", Oid: 25, Value: &pb.Field_Binary{Binary: []byte{'E'}}},
			},
		}},
		verify: func(t *testing.T) {
			var (
				f2 int
				f3 string
			)
			err := conn.QueryRow(ctx, "select f2, f3 from t5 where f1 = $1", 20).Scan(&f2, &f3)
			if err != nil {
				t.Fatal(err)
			}
			if f2 != 21 || f3 != "E" {
				t.Fatalf("unexpected value for (f2, f3): (%v, %v)", f2, f3)
			}
		},
	})

	doTx(task{
		chs: []*pb.Change{{
			Op:     pb.Change_UPDATE,
			Schema: "public",
			Table:  "t5",
			Old: []*pb.Field{
				{Name: "f1", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 20}}},
				{Name: "f2", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 21}}},
				{Name: "f3", Oid: 25, Value: &pb.Field_Binary{Binary: []byte{'E'}}},
			},
			New: []*pb.Field{
				{Name: "f1", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 20}}},
				{Name: "f2", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 22}}},
				{Name: "f3", Oid: 25, Value: &pb.Field_Binary{Binary: []byte{'F'}}},
			},
		}},
		verify: func(t *testing.T) {
			var (
				f2 int
				f3 string
			)
			err := conn.QueryRow(ctx, "select f2, f3 from t5 where f1 = $1", 20).Scan(&f2, &f3)
			if err != nil {
				t.Fatal(err)
			}
			if f2 != 22 || f3 != "F" {
				t.Fatalf("unexpected value for (f2, f3): (%v, %v)", f2, f3)
			}
		},
	})

	doTx(task{
		chs: []*pb.Change{{
			Op:     pb.Change_INSERT,
			Schema: decode.ExtensionSchema,
			Table:  decode.ExtensionDDLLogs,
			New: []*pb.Field{
				{Name: "query", Value: &pb.Field_Binary{Binary: []byte(`create table t6 (f1 int generated always as identity primary key, f2 int, f3 int generated always as (f2 + 1) stored, f4 text)`)}},
				{Name: "tags", Value: &pb.Field_Binary{Binary: tags("CREATE TABLE")}},
			},
		}},
		// the tests for the generated columns are only for pg12 or above
		minVer: 120000,
	})

	doTx(task{
		chs: []*pb.Change{{
			Op:     pb.Change_INSERT,
			Schema: "public",
			Table:  "t6",
			New: []*pb.Field{
				{Name: "f1", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 20}}},
				{Name: "f2", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 20}}},
				{Name: "f3", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 100}}},
				{Name: "f4", Oid: 25, Value: &pb.Field_Binary{Binary: []byte{'A'}}},
			},
		}},
		verify: func(t *testing.T) {
			var (
				f2 int
				f3 int
				f4 string
			)
			// should override the system value for generated identity column f1
			err := conn.QueryRow(ctx, "select f2, f3, f4 from t6 where f1 = $1", 20).Scan(&f2, &f3, &f4)
			if err != nil {
				t.Fatal(err)
			}
			// should still get the generated value for f3
			if f2 != 20 || f3 != 21 || f4 != "A" {
				t.Fatalf("unexpected value for (f2, f3, f4): (%v, %v, %v)", f2, f3, f4)
			}
		},
		minVer: 120000,
	})

	doTx(task{
		chs: []*pb.Change{{
			Op:     pb.Change_UPDATE,
			Schema: "public",
			Table:  "t6",
			New: []*pb.Field{
				{Name: "f1", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 20}}},
				{Name: "f2", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 30}}},
				{Name: "f3", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 100}}},
				{Name: "f4", Oid: 25, Value: &pb.Field_Binary{Binary: []byte{'B'}}},
			},
		}},
		verify: func(t *testing.T) {
			var (
				f2 int
				f3 int
				f4 string
			)
			// should override the system value for generated identity column f1
			err := conn.QueryRow(ctx, "select f2, f3, f4 from t6 where f1 = $1", 20).Scan(&f2, &f3, &f4)
			if err != nil {
				t.Fatal(err)
			}
			// should still get the generated value for f3
			if f2 != 30 || f3 != 31 || f4 != "B" {
				t.Fatalf("unexpected value for (f2, f3, f4): (%v, %v, %v)", f2, f3, f4)
			}
		},
		minVer: 120000,
	})

	doTx(task{
		chs: []*pb.Change{{
			Op:     pb.Change_UPDATE,
			Schema: "public",
			Table:  "t6",
			Old: []*pb.Field{
				{Name: "f1", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 20}}},
				{Name: "f2", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 30}}},
				{Name: "f3", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 31}}},
				{Name: "f4", Oid: 25, Value: &pb.Field_Binary{Binary: []byte{'B'}}},
			},
			New: []*pb.Field{
				{Name: "f1", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 20}}},
				{Name: "f2", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 40}}},
				{Name: "f3", Oid: 23, Value: &pb.Field_Binary{Binary: []byte{0, 0, 0, 100}}},
				{Name: "f4", Oid: 25, Value: &pb.Field_Binary{Binary: []byte{'C'}}},
			},
		}},
		verify: func(t *testing.T) {
			var (
				f2 int
				f3 int
				f4 string
			)
			// should override the system value for generated identity column f1
			err := conn.QueryRow(ctx, "select f2, f3, f4 from t6 where f1 = $1", 20).Scan(&f2, &f3, &f4)
			if err != nil {
				t.Fatal(err)
			}
			// should still get the generated value for f3
			if f2 != 40 || f3 != 41 || f4 != "C" {
				t.Fatalf("unexpected value for (f2, f3, f4): (%v, %v, %v)", f2, f3, f4)
			}
		},
		minVer: 120000,
	})

	sink.Stop()

	// test restart checkpoint
	sink = newPGXSink()

	cp, err = sink.Setup()
	if err != nil {
		t.Fatal(err)
	}
	if cp.LSN != lsn || string(cp.Data) != now.Format(time.RFC3339Nano) {
		t.Fatalf("unexpected %v %v %v", cp, lsn, now)
	}
	sink.Stop()
}

func tags(v ...string) []byte {
	t := pgtype.TextArray{}
	t.Set(v)
	buf, _ := t.EncodeBinary(pgtype.NewConnInfo(), nil)
	return buf
}

func TestPGXSink_DuplicatedSink(t *testing.T) {
	sink1 := newPGXSink()
	if _, err := sink1.Setup(); err != nil {
		t.Fatal(err)
	}
	defer sink1.Stop()

	sink2 := newPGXSink()
	if _, err := sink2.Setup(); err == nil || !strings.Contains(err.Error(), "occupying") {
		t.Fatal("duplicated sink")
	}
}

func TestPGXSink_ScanCheckpointFromLog(t *testing.T) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, test.GetPostgresURL())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	conn.Exec(ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public")
	conn.Exec(ctx, "DROP EXTENSION IF EXISTS pgcapture")

	tmp, err := ioutil.TempFile("", "postgres.sql")
	if err != nil {
		t.Fatal(err)
	}
	tmp.WriteString("2021-03-01 16:25:02 UTC [2152-1] postgres@postgres FATAL:  the database system is starting up\n" +
		"2021-03-01 16:25:02 UTC [1934-5] LOG:  consistent recovery state reached at AE28/49A509D8\n" +
		"2021-03-01 16:25:02 UTC [1934-6] LOG:  invalid record length at AE28/49B13618: wanted 24, got 0\n" +
		"2021-03-01 16:25:02 UTC [1934-7] LOG:  redo done at AE28/49B135E8\n" +
		"2021-03-01 16:25:02 UTC [1934-8] LOG:  last completed transaction was at log time 2021-03-01 16:17:48.597172+00\n")
	defer os.Remove(tmp.Name())
	defer tmp.Close()

	go func() {
		for i := 0; i < 10000; i++ {
			tmp.WriteString("2021-03-01 16:25:03 UTC [2163-1] postgres@postgres FATAL:  the database system is starting up\n")
		}
	}()
	reader, err := os.Open(tmp.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	sink := newPGXSink()
	sink.LogReader = reader

	cp, err := sink.Setup()
	if err != nil {
		t.Fatal(err)
	}
	lsn, _ := pglogrepl.ParseLSN("AE28/49B135E8")
	if cp.LSN != uint64(lsn) || string(cp.Data) != "2021-03-01T16:17:48.597172Z" {
		t.Fatalf("unexpected %v", cp)
	}
	sink.Stop()
}
