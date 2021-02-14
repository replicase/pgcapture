package sink

import "github.com/rueian/pgcapture/pkg/pb"

type Sink interface {
	Setup() (lsn uint64, err error)
	Apply(changes chan *pb.Message) (committed chan uint64)
	Error() error
	Stop()
}
