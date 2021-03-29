package decode

import "github.com/rueian/pgcapture/pkg/pb"

const (
	ExtensionSchema  = "pgcapture"
	ExtensionDDLLogs = "ddl_logs"
	ExtensionSources = "sources"
)

type Decoder interface {
	Decode(in []byte) (*pb.Message, error)
}

func IsDDL(m *pb.Change) bool {
	return m.Schema == ExtensionSchema && m.Table == ExtensionDDLLogs
}

func Ignore(m *pb.Change) bool {
	return m.Schema == ExtensionSchema && m.Table == ExtensionSources
}
