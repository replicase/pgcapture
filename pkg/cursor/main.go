package cursor

import (
	"errors"
	"strconv"
	"strings"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/jackc/pglogrepl"
)

type Checkpoint struct {
	LSN  uint64
	Seq  uint32
	Data []byte
}

func (cp *Checkpoint) Equal(cp2 Checkpoint) bool {
	return cp.LSN == cp2.LSN && cp.Seq == cp2.Seq
}

func (cp *Checkpoint) After(cp2 Checkpoint) bool {
	return (cp.LSN > cp2.LSN) || (cp.LSN == cp2.LSN && cp.Seq > cp2.Seq)
}

func (cp *Checkpoint) ToKey() string {
	return pglogrepl.LSN(cp.LSN).String() + "|" + strconv.FormatUint(uint64(cp.Seq), 16)
}

func (cp *Checkpoint) FromKey(str string) error {
	parts := strings.Split(str, "|")
	if len(parts) != 2 {
		return errors.New("malformed key, should be lsn|seq")
	}
	lsn, err := pglogrepl.ParseLSN(parts[0])
	if err != nil {
		return err
	}
	seq, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return err
	}
	cp.LSN = uint64(lsn)
	cp.Seq = uint32(seq)
	return nil
}

func ToCheckpoint(msg pulsar.Message) (cp Checkpoint, err error) {
	if err = cp.FromKey(msg.Key()); err != nil {
		return
	}
	cp.Data = msg.ID().Serialize()
	return
}
