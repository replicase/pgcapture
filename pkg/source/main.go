package source

import (
	"github.com/rueian/pgcapture/pkg/pb"
)

type Source interface {
	Setup() error
	Capture(lsn uint64) (changes chan Change, err error)
	Commit(lsn uint64)
	Stop()
}

type Change struct {
	LSN     uint64
	Message *pb.Message
}
