package decode

import (
	"encoding/binary"
	"errors"
	"github.com/rueian/pgcapture/pkg/pb"
)

var OpMap = map[byte]pb.Change_Operation{
	'I': pb.Change_INSERT,
	'U': pb.Change_UPDATE,
	'D': pb.Change_DELETE,
}

func NewPGLogicalDecoder(schema *PGXSchemaLoader) *PGLogicalDecoder {
	return &PGLogicalDecoder{
		schema:    schema,
		relations: make(map[uint32]Relation),
	}
}

type PGLogicalDecoder struct {
	schema    *PGXSchemaLoader
	relations map[uint32]Relation
}

func (p *PGLogicalDecoder) Decode(in []byte) (m *pb.Message, err error) {
	switch in[0] {
	case 'B':
		return ReadBegin(in)
	case 'C':
		return ReadCommit(in)
	case 'R':
		r := Relation{}
		err = ReadRelation(in, &r)
		p.relations[r.Rel] = r
	case 'I', 'U', 'D':
		r := RowChange{}
		if err = ReadRowChange(in, &r); err != nil {
			return nil, err
		}

		rel, ok := p.relations[r.Rel]
		if !ok {
			return nil, errors.New("relation not found")
		}

		c := &pb.Change{Namespace: rel.NspName, Table: rel.RelName, Op: OpMap[in[0]]}

		if r.Old != nil {
			if c.OldTuple, err = p.makePBTuple(rel, r.Old); err != nil {
				return nil, err
			}
		}

		if r.New != nil {
			if c.NewTuple, err = p.makePBTuple(rel, r.New); err != nil {
				return nil, err
			}
		}
		return &pb.Message{Type: &pb.Message_Change{Change: c}}, nil
	default:
		// TODO log unmatched message
	}
	return nil, err
}

func (p *PGLogicalDecoder) makePBTuple(rel Relation, src []Field) (fields []*pb.Field, err error) {
	fields = make([]*pb.Field, 0, len(src))
	for i, s := range src {
		oid, err := p.schema.GetTypeOID(rel.NspName, rel.RelName, rel.Fields[i])
		if err != nil {
			return nil, err
		}
		switch s.Format {
		case 'n', 'b':
			fields = append(fields, &pb.Field{Name: rel.Fields[i], Oid: oid, Datum: s.Datum})
		case 'u':
			continue // unchanged toast field should be exclude
		}
	}
	return fields, nil
}

func ReadBegin(in []byte) (*pb.Message, error) {
	if len(in) != 1+1+8+8+4 {
		return nil, errors.New("begin wrong length")
	}
	return &pb.Message{Type: &pb.Message_Begin{Begin: &pb.Begin{
		FinalLsn:   binary.BigEndian.Uint64(in[2:10]),
		CommitTime: binary.BigEndian.Uint64(in[10:18]),
		RemoteXid:  binary.BigEndian.Uint32(in[18:]),
	}}}, nil
}

func ReadCommit(in []byte) (*pb.Message, error) {
	if len(in) != 1+1+8+8+8 {
		return nil, errors.New("commit wrong length")
	}
	return &pb.Message{Type: &pb.Message_Commit{Commit: &pb.Commit{
		CommitLsn:  binary.BigEndian.Uint64(in[2:10]),
		EndLsn:     binary.BigEndian.Uint64(in[10:18]),
		CommitTime: binary.BigEndian.Uint64(in[18:]),
	}}}, nil
}

func ReadRelation(in []byte, m *Relation) (err error) {
	reader := NewBytesReader(in)
	reader.Skip(2) // skip op and flags

	m.Rel, err = reader.Uint32()
	m.NspName, err = reader.String8()
	m.RelName, err = reader.String8()

	if t, err := reader.Byte(); err != nil || t != 'A' {
		return errors.New("relation expected A, got " + string(t))
	}

	n, err := reader.Int16()
	m.Fields = make([]string, n)
	for i := 0; i < n; i++ {
		if t, err := reader.Byte(); err != nil || t != 'C' {
			return errors.New("relation expected C, got " + string(t))
		}
		reader.Skip(1) // skip flags
		if t, err := reader.Byte(); err != nil || t != 'N' {
			return errors.New("relation expected N, got " + string(t))
		}
		m.Fields[i], err = reader.String16()
	}
	return err
}

func ReadRowChange(in []byte, m *RowChange) (err error) {
	reader := NewBytesReader(in)
	m.Op, err = reader.Byte()
	reader.Skip(1) // skip flags
	m.Rel, err = reader.Uint32()

	kind, err := reader.Byte()
	if kind != 'N' {
		m.Old, err = readTuple(reader)
		if m.Op == 'U' {
			kind, err = reader.Byte()
		}
	}
	if kind == 'N' {
		m.New, err = readTuple(reader)
	}
	return err
}

func readTuple(reader *BytesReader) (fields []Field, err error) {
	if t, err := reader.Byte(); err != nil || t != 'T' {
		return nil, errors.New("expect T for tuple message, got " + string(t))
	}

	if n, err := reader.Int16(); err == nil {
		fields = make([]Field, n)
	}

	for i := range fields {
		if fields[i].Format, err = reader.Byte(); err != nil {
			return nil, err
		}
		switch fields[i].Format {
		case 'b':
			fields[i].Datum, err = reader.Bytes32()
		case 'n', 'u':
			continue
		default:
			return nil, errors.New("unsupported data format: " + string(fields[i].Format))
		}
	}
	return
}

type Relation struct {
	Rel     uint32
	NspName string
	RelName string
	Fields  []string
}

type RowChange struct {
	Op  byte
	Rel uint32
	Old []Field
	New []Field
}

type Field struct {
	Format byte
	Datum  []byte
}
