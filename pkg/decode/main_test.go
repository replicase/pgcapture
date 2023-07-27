package decode

import (
	"bytes"
	"context"
	"encoding/hex"
	"strings"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/rueian/pgcapture/pkg/pb"
)

const TestSlot = "test_slot"

type change struct {
	Expect *pb.Change
}

func (c *change) Apply(ctx context.Context, conn *pgx.Conn) (err error) {
	vals := make([][]byte, 6)
	fmts := make([]int16, 6)
	oids := make([]uint32, 6)

	for i, t := range c.Expect.New {
		vals[i] = t.GetBinary()
		oids[i] = t.Oid
		fmts[i] = 1
	}

	switch c.Expect.Op {
	case pb.Change_INSERT:
		_, err = conn.PgConn().ExecParams(ctx, "insert into t values ($1,$2,$3,$4,$5,$6)", vals, oids, fmts, fmts).Close()
	case pb.Change_UPDATE:
		if c.Expect.Old != nil {
			vals[5] = c.Expect.Old[0].GetBinary()
			oids[5] = c.Expect.Old[0].Oid
		} else {
			vals[5] = c.Expect.New[0].GetBinary()
			oids[5] = c.Expect.New[0].Oid
		}
		fmts[5] = 1
		_, err = conn.PgConn().ExecParams(ctx, "update t set id=$1,uid=$2,txt=$3,js=$4,ts=$5 where id=$6", vals, oids, fmts, fmts).Close()
	case pb.Change_DELETE:
		vals[0] = c.Expect.Old[0].GetBinary()
		oids[0] = c.Expect.Old[0].Oid
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
