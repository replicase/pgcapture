package decode

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/sql"
)

type fieldSet struct {
	set map[string]struct{}
}

func fieldSetWithList(list pgtype.TextArray) fieldSet {
	s := fieldSet{set: make(map[string]struct{}, len(list.Elements))}
	for _, v := range list.Elements {
		s.append(v.String)
	}
	return s
}

func (s fieldSet) Contains(f string) bool {
	_, ok := s.set[f]
	return ok
}

func (s fieldSet) append(f string) {
	s.set[f] = struct{}{}
}

func (s fieldSet) list() []string {
	list := make([]string, 0, len(s.set))
	for k := range s.set {
		list = append(list, k)
	}
	return list
}

func (s fieldSet) Len() int {
	return len(s.set)
}

type ColumnInfo struct {
	keys                   fieldSet
	identityGenerationList fieldSet
	generatedList          fieldSet
}

func (i ColumnInfo) IsGenerated(f string) bool {
	return i.generatedList.Contains(f)
}

func (i ColumnInfo) IsIdentityGeneration(f string) bool {
	return i.identityGenerationList.Contains(f)
}

func (i ColumnInfo) IsKey(f string) bool {
	return i.keys.Contains(f)
}

func (i ColumnInfo) ListKeys() []string {
	return i.keys.list()
}

func (i ColumnInfo) KeyLength() int {
	return i.keys.Len()
}

func (i ColumnInfo) isEmpty() bool {
	return i.keys.Len() == 0 && i.generatedList.Len() == 0 && i.identityGenerationList.Len() == 0
}

type fieldSelector func(i ColumnInfo, field string) bool

func (i ColumnInfo) Filter(fields []*pb.Field, fieldSelector fieldSelector) (fieldSet, []*pb.Field) {
	if i.isEmpty() {
		return fieldSet{}, fields
	}
	cols := make([]string, 0, len(fields))
	fFields := make([]*pb.Field, 0, len(fields))
	for _, f := range fields {
		if fieldSelector(i, f.Name) {
			cols = append(cols, f.Name)
			fFields = append(fFields, f)
		}
	}

	set := fieldSet{set: make(map[string]struct{}, len(cols))}
	for _, f := range cols {
		set.append(f)
	}
	return set, fFields
}

type TypeCache map[string]map[string]map[string]uint32
type KeysCache map[string]map[string]ColumnInfo

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

func (p *PGXSchemaLoader) RefreshColumnInfo() error {
	rows, err := p.conn.Query(context.Background(), sql.QueryIdentityKeys)
	if err != nil {
		return err
	}
	defer rows.Close()

	var nspname, relname string
	for rows.Next() {
		var (
			keys                      pgtype.TextArray
			identityGenerationColumns pgtype.TextArray
			generatedColumns          pgtype.TextArray
		)
		if err := rows.Scan(&nspname, &relname, &keys, &identityGenerationColumns, &generatedColumns); err != nil {
			return err
		}
		tbls, ok := p.iKeys[nspname]
		if !ok {
			tbls = make(map[string]ColumnInfo)
			p.iKeys[nspname] = tbls
		}

		tbls[relname] = ColumnInfo{
			keys:                   fieldSetWithList(keys),
			identityGenerationList: fieldSetWithList(identityGenerationColumns),
			generatedList:          fieldSetWithList(generatedColumns),
		}
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

func (p *PGXSchemaLoader) GetColumnInfo(namespace, table string) (*ColumnInfo, error) {
	if tbls, ok := p.iKeys[namespace]; !ok {
		return nil, fmt.Errorf("%s.%s %w", namespace, table, ErrSchemaIdentityMissing)
	} else if info, ok := tbls[table]; !ok {
		return nil, fmt.Errorf("%s.%s %w", namespace, table, ErrSchemaIdentityMissing)
	} else {
		return &info, nil
	}
}

func (p *PGXSchemaLoader) GetTableKey(namespace, table string) (keys []string, err error) {
	if tbls, ok := p.iKeys[namespace]; !ok {
		return nil, fmt.Errorf("%s.%s %w", namespace, table, ErrSchemaIdentityMissing)
	} else if info, ok := tbls[table]; !ok {
		return nil, fmt.Errorf("%s.%s %w", namespace, table, ErrSchemaIdentityMissing)
	} else {
		return info.ListKeys(), nil
	}
}

func (p *PGXSchemaLoader) GetVersion() (version int64, err error) {
	var versionInfo string
	if err = p.conn.QueryRow(context.Background(), sql.ServerVersionNum).Scan(&versionInfo); err != nil {
		return -1, err
	}
	svn, err := strconv.ParseInt(versionInfo, 10, 64)
	if err != nil {
		return -1, err
	}
	return svn, nil
}

var (
	ErrSchemaTableMissing    = errors.New("table missing")
	ErrSchemaColumnMissing   = errors.New("column missing")
	ErrSchemaIdentityMissing = errors.New("table identity keys missing")
)
