package pgcapture

import (
	"context"
	"database/sql"
	"reflect"
	"time"

	"github.com/jackc/pgtype"
	"github.com/rueian/pgcapture/pkg/cursor"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/source"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

const TableRegexOption = "TableRegex"

var DefaultErrorFn = func(source source.Change, err error) {}

func NewSimpleConsumer(ctx context.Context, src source.RequeueSource, option ConsumerOption) *Consumer {
	return newConsumer(ctx, src, option)
}

func NewDBLogConsumer(ctx context.Context, conn *grpc.ClientConn, option ConsumerOption) *Consumer {
	parameters, _ := structpb.NewStruct(map[string]interface{}{})
	if option.TableRegex != "" {
		parameters.Fields[TableRegexOption] = structpb.NewStringValue(option.TableRegex)
	}
	c := &DBLogGatewayConsumer{client: pb.NewDBLogGatewayClient(conn), init: &pb.CaptureInit{
		Uri:        option.URI,
		Parameters: parameters,
	}}
	c.ctx, c.cancel = context.WithCancel(ctx)

	return newConsumer(c.ctx, c, option)
}

/*
NewConsumer
Deprecated: please use NewDBLogConsumer instead
*/
func NewConsumer(ctx context.Context, conn *grpc.ClientConn, option ConsumerOption) *Consumer {
	return NewDBLogConsumer(ctx, conn, option)
}

func newConsumer(ctx context.Context, src source.RequeueSource, option ConsumerOption) *Consumer {
	errFn := DefaultErrorFn
	if option.OnDecodeError != nil {
		errFn = option.OnDecodeError
	}

	consumer := &Consumer{ctx: ctx, Source: src, errFn: errFn}
	if option.DebounceInterval > 0 {
		consumer.Bouncer = &DebounceHandler{
			Interval: option.DebounceInterval,
			source:   src,
		}
	} else {
		consumer.Bouncer = &NoBounceHandler{source: src}
	}

	return consumer
}

type OnDecodeError func(source source.Change, err error)

type ConsumerOption struct {
	URI              string
	TableRegex       string
	DebounceInterval time.Duration
	OnDecodeError    OnDecodeError
}

type Consumer struct {
	Source  source.RequeueSource
	Bouncer BounceHandler
	ctx     context.Context
	errFn   OnDecodeError
}

func (c *Consumer) ConsumeAsync(mh ModelAsyncHandlers) error {
	if err := c.Bouncer.Initialize(c.ctx, mh); err != nil {
		return err
	}

	refs := make(map[string]reflection, len(mh))
	for m, h := range mh {
		ref, err := reflectModel(m)
		if err != nil {
			return err
		}
		ref.hdl = h
		refs[ModelName(m.TableName())] = ref
	}

	changes, err := c.Source.Capture(cursor.Checkpoint{})
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
			n, err := makeModel(ref, m.Change.New)
			if err != nil {
				c.errFn(change, err)
				break
			}
			o, err := makeModel(ref, m.Change.Old)
			if err != nil {
				c.errFn(change, err)
				break
			}
			c.Bouncer.Handle(ref.hdl, change.Checkpoint, Change{
				Op:         m.Change.Op,
				Checkpoint: change.Checkpoint,
				New:        n,
				Old:        o,
			})
			continue
		}
		c.Source.Commit(change.Checkpoint)
	}
	return c.Source.Error()
}

func (c *Consumer) Consume(mh ModelHandlers) error {
	mah := make(ModelAsyncHandlers, len(mh))
	for m, fn := range mh {
		mah[m] = toAsyncHandlerFunc(fn)
	}
	return c.ConsumeAsync(mah)
}

func makeModel(ref reflection, fields []*pb.Field) (interface{}, error) {
	ptr := reflect.New(ref.typ)
	val := ptr.Elem()
	interfaces := make(map[string]interface{}, len(ref.idx))
	for name, i := range ref.idx {
		if f := val.Field(i).Addr(); f.CanInterface() {
			interfaces[name] = f.Interface()
		}
	}
	var err error
	for _, f := range fields {
		field, ok := interfaces[f.Name]
		if !ok {
			continue
		}
		if f.Value == nil {
			if decoder, ok := field.(pgtype.BinaryDecoder); ok {
				err = decoder.DecodeBinary(ci, nil)
			} else if scanner, ok := field.(sql.Scanner); ok {
				err = scanner.Scan(nil)
			}
		} else {
			if value, ok := f.Value.(*pb.Field_Binary); ok {
				if decoder, ok := field.(pgtype.BinaryDecoder); ok {
					err = decoder.DecodeBinary(ci, value.Binary)
				} else if scanner, ok := field.(sql.Scanner); ok {
					err = scanner.Scan(value.Binary)
				}
			} else {
				if decoder, ok := field.(pgtype.TextDecoder); ok {
					err = decoder.DecodeText(ci, []byte(f.GetText()))
				} else if scanner, ok := field.(sql.Scanner); ok {
					err = scanner.Scan(f.GetText())
				}
			}
		}
		if err != nil {
			return nil, err
		}
	}
	return ptr.Interface(), nil
}

func (c *Consumer) Stop() {
	c.Source.Stop()
}

var ci = pgtype.NewConnInfo()
