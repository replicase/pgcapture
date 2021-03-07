package dblog

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v4"
	"github.com/rueian/pgcapture/pkg/pb"
)

type PGXSourceDumper struct {
	Conn *pgx.Conn
}

func (p *PGXSourceDumper) LoadDump(minLSN uint64, info *pb.DumpInfoResponse) ([]*pb.Change, error) {
	for {
		changes, err := p.load(minLSN, info)
		if err == errRetry {
			time.Sleep(time.Millisecond * 100)
			continue
		}
		if err != nil {
			return nil, err
		}
		return changes, nil
	}
}

const DumpQuery = `select * from "%s"."%s" where ctid = any(array(select format('(%%s,%%s)', i, j)::tid from generate_series($1::int,$2::int) as gs(i), generate_series(1,(current_setting('block_size')::int-24)/28) as gs2(j)))`

func (p *PGXSourceDumper) load(minLSN uint64, info *pb.DumpInfoResponse) ([]*pb.Change, error) {
	ctx := context.Background()

	tx, err := p.Conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	if err = checkLSN(ctx, tx, minLSN); err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx, fmt.Sprintf(DumpQuery, info.Namespace, info.Table), info.PageBegin, info.PageEnd)
	if err != nil {
		return nil, err
	}

	var changes []*pb.Change
	for rows.Next() {
		values := rows.RawValues()
		change := &pb.Change{Op: pb.Change_UPDATE, Namespace: info.Namespace, Table: info.Table}
		for i, fd := range rows.FieldDescriptions() {
			change.NewTuple = append(change.NewTuple, &pb.Field{Name: string(fd.Name), Oid: fd.DataTypeOID, Datum: values[i]})
		}
		changes = append(changes, change)
	}
	return changes, nil
}

func checkLSN(ctx context.Context, tx pgx.Tx, minLSN uint64) (err error) {
	var str string
	if err = tx.QueryRow(ctx, "SELECT MAX(commit) FROM pgcapture.sources WHERE status IS NULL").Scan(&str); err != nil {
		return err
	}

	lsn, err := pglogrepl.ParseLSN(str)
	if err != nil {
		return err
	}

	if uint64(lsn) < minLSN {
		return errRetry
	}
	return nil
}

var errRetry = errors.New("retry")
