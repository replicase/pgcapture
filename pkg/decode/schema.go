package decode

import (
	"context"
	"errors"
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

func (p *PGXSchemaLoader) GetTypeOID(namespace, table, field string) (uint32, error) {
	if tbls, ok := p.cache[namespace]; ok {
		if cols, ok := tbls[table]; ok {
			if oid, ok := cols[field]; ok {
				return oid, nil
			}
		}
	}
	return 0, errors.New("type oid not found")
}
