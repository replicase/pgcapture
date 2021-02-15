package decode

import "github.com/rueian/pgcapture/pkg/pb"

type Decoder interface {
	Decode(in []byte) (*pb.Message, error)
}

func IsDDL(m *pb.Change) bool {
	return m.Namespace == "pgcapture" && m.Table == "ddl_logs"
}

func Ignore(m *pb.Change) bool {
	return m.Namespace == "pgcapture" && m.Table == "sources"
}
