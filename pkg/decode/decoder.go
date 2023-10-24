package decode

import (
	"github.com/replicase/pgcapture/pkg/pb"
)

const (
	ExtensionSchema  = "pgcapture"
	ExtensionDDLLogs = "ddl_logs"
	ExtensionSources = "sources"
)

const (
	PGLogicalOutputPlugin = "pglogical_output"
	PGOutputPlugin        = "pgoutput"
)

var OpMap = map[byte]pb.Change_Operation{
	'I': pb.Change_INSERT,
	'U': pb.Change_UPDATE,
	'D': pb.Change_DELETE,
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

type Decoder interface {
	Decode(in []byte) (*pb.Message, error)
	GetPluginArgs() []string
}

func IsDDL(m *pb.Change) bool {
	return m.Schema == ExtensionSchema && m.Table == ExtensionDDLLogs
}

func Ignore(m *pb.Change) bool {
	return m.Schema == ExtensionSchema && m.Table == ExtensionSources
}

func makeOldPBTuple(schema *PGXSchemaLoader, rel Relation, src []Field, noNull bool) (fields []*pb.Field) {
	if src == nil {
		return nil
	}
	fields = make([]*pb.Field, 0, len(src))
	for i, s := range src {
		if noNull && s.Datum == nil {
			continue
		}
		typeInfo, err := schema.GetTypeInfo(rel.NspName, rel.RelName, rel.Fields[i])
		if err != nil {
			// TODO: add optional logging, because it will generate a lot of logs when refreshing materialized view
			continue
		}
		switch s.Format {
		case 'b':
			fields = append(fields, &pb.Field{Name: rel.Fields[i], Oid: typeInfo.OID, Value: &pb.Field_Binary{Binary: s.Datum}})
		case 'n':
			fields = append(fields, &pb.Field{Name: rel.Fields[i], Oid: typeInfo.OID, Value: nil})
		case 't':
			fields = append(fields, &pb.Field{Name: rel.Fields[i], Oid: typeInfo.OID, Value: &pb.Field_Text{Text: string(s.Datum)}})
		case 'u':
			continue // unchanged toast field should be excluded
		}
	}
	return fields
}

func makeNewPBTuple(schema *PGXSchemaLoader, rel Relation, old, new []Field, noNull bool) (fields []*pb.Field) {
	if new == nil {
		return nil
	}
	fields = make([]*pb.Field, 0, len(new))
	for i, s := range new {
		if noNull && s.Datum == nil {
			continue
		}
		typeInfo, err := schema.GetTypeInfo(rel.NspName, rel.RelName, rel.Fields[i])
		if err != nil {
			// TODO: add optional logging, because it will generate a lot of logs when refreshing materialized view
			continue
		}
	ReAppend:
		switch s.Format {
		case 'b':
			fields = append(fields, &pb.Field{Name: rel.Fields[i], Oid: typeInfo.OID, Value: &pb.Field_Binary{Binary: s.Datum}})
		case 'n':
			fields = append(fields, &pb.Field{Name: rel.Fields[i], Oid: typeInfo.OID, Value: nil})
		case 't':
			fields = append(fields, &pb.Field{Name: rel.Fields[i], Oid: typeInfo.OID, Value: &pb.Field_Text{Text: string(s.Datum)}})
		case 'u':
			// fill the unchanged field with old value when ReplicaIdentity is full
			// otherwise, skip the unchanged field
			if typeInfo.ReplicaIdentity == ReplicaIdentityFull {
				s.Format = old[i].Format
				s.Datum = old[i].Datum
				goto ReAppend
			}
			continue
		}
	}
	return fields
}
