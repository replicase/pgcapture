package decode

import "github.com/rueian/pgcapture/pkg/pb"

type Decoder interface {
	Decode(in []byte) (*pb.Message, error)
}
