package decode

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/rueian/pgcapture/pkg/sql"
)

type TypeCache map[string]map[string]map[string]uint32
type KeysCache map[string]map[string][]string

func NewPGXSchemaLoader(conn *pgx.Conn) *PGXSchemaLoader {
	return &PGXSchemaLoader{conn: conn, types: make(TypeCache), iKeys: make(KeysCache)}
}

type PGXSchemaLoader struct {
	conn  *pgx.Conn
	types TypeCache
	iKeys KeysCache
}

func (p *PGXSchemaLoader) RefreshType() error {
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
		tbls, ok := p.types[nspname]
		if !ok {
			tbls = make(map[string]map[string]uint32)
			p.types[nspname] = tbls
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

func (p *PGXSchemaLoader) RefreshKeys() error {
	rows, err := p.conn.Query(context.Background(), sql.QueryIdentityKeys)
	if err != nil {
		return err
	}
	defer rows.Close()

	var nspname, relname string
	for rows.Next() {
		var keys pgtype.TextArray
		if err := rows.Scan(&nspname, &relname, &keys); err != nil {
			return err
		}
		tbls, ok := p.iKeys[nspname]
		if !ok {
			tbls = make(map[string][]string)
			p.iKeys[nspname] = tbls
		}
		els := make([]string, len(keys.Elements))
		for i, e := range keys.Elements {
			els[i] = e.String
		}
		tbls[relname] = els
	}
	return nil
}

func (p *PGXSchemaLoader) GetTypeOID(namespace, table, field string) (oid uint32, err error) {
	if tbls, ok := p.types[namespace]; !ok {
		return 0, fmt.Errorf("%s.%s %w", namespace, table, ErrSchemaTableMissing)
	} else if cols, ok := tbls[table]; !ok {
		return 0, fmt.Errorf("%s.%s %w", namespace, table, ErrSchemaTableMissing)
	} else if oid, ok = cols[field]; !ok {
		return 0, fmt.Errorf("%s.%s.%s %w", namespace, table, field, ErrSchemaColumnMissing)
	}
	return oid, nil
}

func (p *PGXSchemaLoader) GetTableKey(namespace, table string) (keys []string, err error) {
	if tbls, ok := p.iKeys[namespace]; !ok {
		return nil, fmt.Errorf("%s.%s %w", namespace, table, ErrSchemaIdentityMissing)
	} else if keys, ok = tbls[table]; !ok {
		return nil, fmt.Errorf("%s.%s %w", namespace, table, ErrSchemaIdentityMissing)
	}
	return keys, nil
}

var (
	ErrSchemaTableMissing    = errors.New("table missing")
	ErrSchemaColumnMissing   = errors.New("column missing")
	ErrSchemaIdentityMissing = errors.New("table identity keys missing")
)
