package decode

import (
	"github.com/jackc/pgx/v4"
)

func NewPGXSchemaLoader(conn *pgx.Conn) *PGXSchemaLoader {
	return &PGXSchemaLoader{conn: conn}
}

type PGXSchemaLoader struct {
	conn *pgx.Conn
}

func (p *PGXSchemaLoader) Refresh() error {
	return nil
}

func (p *PGXSchemaLoader) GetFieldOID(namespace, table, field string) (uint32, error) {
	return 0, nil
}
