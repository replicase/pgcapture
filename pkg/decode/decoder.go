package decode

import "github.com/rueian/pgcapture/pkg/pb"

const (
	ExtensionNamespace = "pgcapture"
	ExtensionDDLLogs   = "ddl_logs"
	ExtensionSources   = "sources"
)

type Decoder interface {
	Decode(in []byte) (*pb.Message, error)
}

func IsDDL(m *pb.Change) bool {
	return m.Namespace == ExtensionNamespace && m.Table == ExtensionDDLLogs
}

func Ignore(m *pb.Change) bool {
	return m.Namespace == ExtensionNamespace && m.Table == ExtensionSources
}
