package dblog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v4"
	"github.com/replicase/pgcapture/pkg/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SourceDumper interface {
	LoadDump(minLSN uint64, info *pb.DumpInfoResponse) ([]*pb.Change, error)
	Stop()
}

func NewAgentSourceDumper(ctx context.Context, url string) (*AgentSource, error) {
	conn, err := grpc.DialContext(ctx, url, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return &AgentSource{
		conn:   conn,
		client: pb.NewAgentClient(conn),
	}, nil
}

type AgentSource struct {
	conn   *grpc.ClientConn
	client pb.AgentClient
}

func (a *AgentSource) LoadDump(minLSN uint64, info *pb.DumpInfoResponse) (changes []*pb.Change, err error) {
	stream, err := a.client.StreamDump(context.Background(), &pb.AgentDumpRequest{
		MinLsn: minLSN,
		Info:   info,
	})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			switch s.Code() {
			case codes.NotFound:
				return nil, ErrMissingTable
			case codes.Unavailable:
				return nil, ErrLSNMissing
			case codes.FailedPrecondition:
				return nil, ErrLSNFallBehind
			}
		}
		return nil, err
	}
	for {
		change, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		changes = append(changes, change)
	}
	stream.CloseSend()
	return changes, nil
}

func (a *AgentSource) Stop() {
	a.conn.Close()
}

func NewPGXSourceDumper(ctx context.Context, url string) (*PGXSourceDumper, error) {
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		return nil, err
	}
	return &PGXSourceDumper{conn: conn}, nil
}

type PGXSourceDumper struct {
	conn *pgx.Conn
	mu   sync.Mutex

	SkipLSNCheck bool
}

func (p *PGXSourceDumper) LoadDump(minLSN uint64, info *pb.DumpInfoResponse) ([]*pb.Change, error) {
	if info.Schema == "" || info.Table == "" {
		return nil, ErrMissingTable
	}

	p.mu.Lock()
	changes, err := p.load(minLSN, info)
	p.mu.Unlock()

	if err != nil {
		return nil, err
	}
	return changes, nil
}

func (p *PGXSourceDumper) Stop() {
	p.mu.Lock()
	p.conn.Close(context.Background())
	p.mu.Unlock()
}

// DumpQuery try to generate all possible ctids in a given page range.
// The maximum rip_posid is caculated as current_setting('block_size')::int-24)/28
//
//	where 24 is the size of PageHeaderData, and
//	where 28 is the minimum size of a tuple which is ItemIdData(4 bytes) + HeapTupleHeaderData(23 bytes) with alignment
//	ref: https://github.com/postgres/postgres/blob/c3b011d9918100c6ec2d72297fb51635bce70e80/src/include/access/htup_details.h#L573-L575
const DumpQuery = `select * from "%s"."%s" where ctid = any(array(select format('(%%s,%%s)', i, j)::tid from generate_series($1::int,$2::int) as gs(i), generate_series(1,(current_setting('block_size')::int-24)/28) as gs2(j)))`

func (p *PGXSourceDumper) load(minLSN uint64, info *pb.DumpInfoResponse) ([]*pb.Change, error) {
	ctx := context.Background()

	tx, err := p.conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	if !p.SkipLSNCheck {
		if err = checkLSN(ctx, tx, minLSN); err != nil {
			return nil, err
		}
	}

	rows, err := tx.Query(ctx, fmt.Sprintf(DumpQuery, info.Schema, info.Table), info.PageBegin, info.PageEnd)
	if err != nil {
		var pge *pgconn.PgError
		if errors.As(err, &pge) && pge.Code == "42P01" {
			return nil, ErrMissingTable
		}
		return nil, err
	}

	var changes []*pb.Change
	for rows.Next() {
		values := rows.RawValues()
		change := &pb.Change{Op: pb.Change_UPDATE, Schema: info.Schema, Table: info.Table}
		for i, fd := range rows.FieldDescriptions() {
			if value := values[i]; value == nil {
				change.New = append(change.New, &pb.Field{Name: string(fd.Name), Oid: fd.DataTypeOID, Value: nil})
			} else {
				if fd.Format == 0 {
					change.New = append(change.New, &pb.Field{Name: string(fd.Name), Oid: fd.DataTypeOID, Value: &pb.Field_Text{Text: string(value)}})
				} else {
					change.New = append(change.New, &pb.Field{Name: string(fd.Name), Oid: fd.DataTypeOID, Value: &pb.Field_Binary{Binary: value}})
				}
			}
		}
		changes = append(changes, change)
	}
	return changes, nil
}

func checkLSN(ctx context.Context, tx pgx.Tx, minLSN uint64) (err error) {
	var str string
	var lsn pglogrepl.LSN
	err = tx.QueryRow(ctx, "SELECT commit FROM pgcapture.sources WHERE commit IS NOT NULL ORDER BY commit DESC LIMIT 1").Scan(&str)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrLSNMissing
	}
	if err == nil {
		lsn, err = pglogrepl.ParseLSN(str)
		if err == nil && uint64(lsn) < minLSN {
			return ErrLSNFallBehind
		}
	}
	return err
}

var ErrMissingTable = errors.New("missing Schema or table")
var ErrLSNFallBehind = errors.New("lsn fall behind")
var ErrLSNMissing = errors.New("missing lsn record")
