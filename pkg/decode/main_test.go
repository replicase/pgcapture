package decode

import (
	"bytes"
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/replicase/pgcapture/pkg/pb"
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

func b(in any, oid int) []byte {
	bs, _ := pgtype.NewMap().Encode(uint32(oid), pgtype.BinaryFormatCode, in, nil)
	return bs
}
