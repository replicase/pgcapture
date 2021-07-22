package pgcapture

import (
	"context"
	"reflect"
	"time"

	"github.com/jackc/pgtype"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/source"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

const TableRegexOption = "TableRegex"

var DefaultErrorFn = func(source source.Change, err error) {}

func NewConsumer(ctx context.Context, conn *grpc.ClientConn, option ConsumerOption) *Consumer {
	return newConsumer(ctx, pb.NewDBLogGatewayClient(conn), option)
}

func newConsumer(ctx context.Context, client pb.DBLogGatewayClient, option ConsumerOption) *Consumer {
	parameters, _ := structpb.NewStruct(map[string]interface{}{})
	if option.TableRegex != "" {
		parameters.Fields[TableRegexOption] = structpb.NewStringValue(option.TableRegex)
	}
	c := &DBLogGatewayConsumer{client: client, init: &pb.CaptureInit{
		Uri:        option.URI,
		Parameters: parameters,
	}}
	c.ctx, c.cancel = context.WithCancel(ctx)

	errFn := DefaultErrorFn
	if option.OnDecodeError != nil {
		errFn = option.OnDecodeError
	}

	if option.DebounceInterval > 0 {
		return &Consumer{
			Source: c,
			Bouncer: &DebounceHandler{
				Interval: option.DebounceInterval,
				source:   c,
			},
			ctx:   c.ctx,
			errFn: errFn,
		}
	}

	return &Consumer{Source: c, Bouncer: &NoBounceHandler{source: c}, ctx: c.ctx, errFn: errFn}
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

func (c *Consumer) Consume(mh ModelHandlers) error {
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

	changes, err := c.Source.Capture(source.Checkpoint{})
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
			err = field.(pgtype.BinaryDecoder).DecodeBinary(ci, nil)
		} else {
			if value, ok := f.Value.(*pb.Field_Binary); ok {
				err = field.(pgtype.BinaryDecoder).DecodeBinary(ci, value.Binary)
			} else {
				err = field.(pgtype.TextDecoder).DecodeText(ci, []byte(f.GetText()))
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
