package decode

import "github.com/rueian/pgcapture/pkg/pb"

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
