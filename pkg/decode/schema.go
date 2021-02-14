package decode

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/rueian/pgcapture/pkg/sql"
)

type TypeCache map[string]map[string]map[string]uint32

func NewPGXSchemaLoader(conn *pgx.Conn) *PGXSchemaLoader {
	return &PGXSchemaLoader{conn: conn, cache: make(TypeCache)}
}

type PGXSchemaLoader struct {
	conn  *pgx.Conn
	cache TypeCache
}

func (p *PGXSchemaLoader) Refresh() error {
	rows, err := p.conn.Query(context.Background(), sql.QueryAttrTypeOID)
	if err != nil {
		return err
	}
	defer rows.Close()

	var nspname, relname, attname string
	var atttypid uint32
	for rows.Next() {
		if err := rows.Scan(&nspname, &relname, &attname, &atttypid); err != nil {
			return err
		}
		tbls, ok := p.cache[nspname]
		if !ok {
			tbls = make(map[string]map[string]uint32)
			p.cache[nspname] = tbls
		}
		cols, ok := tbls[relname]
		if !ok {
			cols = make(map[string]uint32)
			tbls[relname] = cols
		}
		cols[attname] = atttypid
	}
	return nil
}

func (p *PGXSchemaLoader) GetTypeOID(namespace, table, field string) (oid uint32, err error) {
	if tbls, ok := p.cache[namespace]; !ok {
		return 0, fmt.Errorf("%s.%s %w", namespace, table, ErrSchemaTableMissing)
	} else if cols, ok := tbls[table]; !ok {
		return 0, fmt.Errorf("%s.%s %w", namespace, table, ErrSchemaTableMissing)
	} else if oid, ok = cols[field]; !ok {
		return 0, fmt.Errorf("%s.%s.%s %w", namespace, table, field, ErrSchemaColumnMissing)
	}
	return oid, nil
}

var (
	ErrSchemaTableMissing  = errors.New("table missing")
	ErrSchemaColumnMissing = errors.New("column missing")
)
