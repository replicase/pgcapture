package eventing

import (
	"reflect"

	"github.com/jackc/pgtype"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/source"
)

type Consumer struct {
	source source.RequeueSource
}

func (c *Consumer) Consume(mh ModelHandlers) error {
	refs := make(map[string]reflection, len(mh))
	for m, h := range mh {
		ref, err := reflectModel(m)
		if err != nil {
			return err
		}
		ref.hdl = h
		refs[ModelName(m.Name())] = ref
	}

	changes, err := c.source.Capture(source.Checkpoint{})
	if err != nil {
		return err
	}

	for change := range changes {
		switch m := change.Message.Type.(type) {
		case *pb.Message_Change:
			ref, ok := refs[ModelName(m.Change.Namespace, m.Change.Table)]
			if !ok {
				break
			}
			if err := ref.hdl(Change{
				Op:  m.Change.Op,
				LSN: change.Checkpoint.LSN,
				New: makeModel(ref, m.Change.NewTuple),
				Old: makeModel(ref, m.Change.OldTuple),
			}); err != nil {
				c.source.Requeue(change.Checkpoint)
				continue
			}
		}
		c.source.Commit(change.Checkpoint)
	}
	return nil
}

func makeModel(ref reflection, fields []*pb.Field) interface{} {
	if len(fields) == 0 {
		return nil
	}
	ptr := reflect.New(ref.typ)
	val := ptr.Elem()
	for _, f := range fields {
		i, ok := ref.idx[f.Name]
		if !ok {
			continue
		}
		if err := val.Field(i).Addr().Interface().(pgtype.BinaryDecoder).DecodeBinary(ci, f.Datum); err != nil {
			return err
		}
	}
	return ptr.Interface()
}

func (c *Consumer) Stop() {
	c.source.Stop()
}

var ci = pgtype.NewConnInfo()
