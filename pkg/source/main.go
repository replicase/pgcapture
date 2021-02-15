package source

import (
	"github.com/rueian/pgcapture/pkg/pb"
	"time"
)

type Checkpoint struct {
	LSN  uint64
	Time time.Time
}

type Change struct {
	LSN     uint64
	Message *pb.Message
}

type Source interface {
	Setup() error
	Capture(cp Checkpoint) (changes chan Change, err error)
	Commit(cp Checkpoint)
	Stop()
}
