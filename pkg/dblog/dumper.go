package dblog

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pglogrepl"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/rueian/pgcapture/pkg/pb"
)

type PGXSourceDumper struct {
	Conn *pgx.Conn
}

func (p *PGXSourceDumper) NextDumpInfo(uri string) (*DumpInfo, error) {
	// TODO
	return &DumpInfo{URI: `{"Source":"debug","Namespace":"public","Table":"t1","Query":"select * from t1"}`}, nil
}

func (p *PGXSourceDumper) LoadDump(minLSN uint64, info *DumpInfo) ([]*pb.Change, error) {
	for {
		changes, err := p.load(minLSN, info)
		if err == errRetry {
			time.Sleep(time.Second)
			continue
		}
		if err != nil {
			return nil, err
		}
		return changes, nil
	}
}

func (p *PGXSourceDumper) load(minLSN uint64, info *DumpInfo) ([]*pb.Change, error) {
	pi := &pgDumpInfo{}
	if err := json.Unmarshal([]byte(info.URI), pi); err != nil {
		return nil, err
	}

	ctx := context.Background()
	tx, err := p.Conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	if err = checkLSN(ctx, tx, pi, minLSN); err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx, pi.Query)
	if err != nil {
		return nil, err
	}

	var changes []*pb.Change
	for rows.Next() {
		values := rows.RawValues()
		change := &pb.Change{
			Op:        pb.Change_UPDATE,
			Namespace: pi.Namespace,
			Table:     pi.Table,
		}
		for i, fd := range rows.FieldDescriptions() {
			change.NewTuple = append(change.NewTuple, &pb.Field{
				Name:  string(fd.Name),
				Oid:   fd.DataTypeOID,
				Datum: values[i],
			})
		}
		changes = append(changes, change)
	}
	return changes, nil
}

func checkLSN(ctx context.Context, tx pgx.Tx, pi *pgDumpInfo, minLSN uint64) (err error) {
	var str string
	if err = tx.QueryRow(ctx, "SELECT commit FROM pgcapture.sources WHERE id = $1 AND status IS NULL", pi.Source).Scan(&str); err != nil {
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

type pgDumpInfo struct {
	Source    string
	Namespace string
	Table     string
	Query     string
}

var errRetry = errors.New("retry")
