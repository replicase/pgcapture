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
		refs[ModelName(m.TableName())] = ref
	}

	changes, err := c.source.Capture(source.Checkpoint{})
	if err != nil {
		return err
	}

	for change := range changes {
		switch m := change.Message.Type.(type) {
		case *pb.Message_Change:
			ref, ok := refs[ModelName(m.Change.Schema, m.Change.Table)]
			if !ok {
				break
			}
			if err := ref.hdl(Change{
				Op:  m.Change.Op,
				LSN: change.Checkpoint.LSN,
				New: makeModel(ref, m.Change.New),
				Old: makeModel(ref, m.Change.Old),
			}); err != nil {
				c.source.Requeue(change.Checkpoint, err.Error())
				continue
			}
		}
		c.source.Commit(change.Checkpoint)
	}
	return c.source.Error()
}

func makeModel(ref reflection, fields []*pb.Field) interface{} {
	if len(fields) == 0 {
		return nil
	}
	ptr := reflect.New(ref.typ)
	val := ptr.Elem()
	var err error
	for _, f := range fields {
		i, ok := ref.idx[f.Name]
		if !ok {
			continue
		}
		if f.Value == nil {
			// do nothing
		} else if value, ok := f.Value.(*pb.Field_Binary); ok {
			err = val.Field(i).Addr().Interface().(pgtype.BinaryDecoder).DecodeBinary(ci, value.Binary)
		} else {
			err = val.Field(i).Addr().Interface().(pgtype.TextDecoder).DecodeText(ci, []byte(f.GetText()))
		}
		if err != nil {
			return err
		}
	}
	return ptr.Interface()
}

func (c *Consumer) Stop() {
	c.source.Stop()
}

var ci = pgtype.NewConnInfo()
