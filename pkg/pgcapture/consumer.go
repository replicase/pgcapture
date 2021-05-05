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

func NewConsumer(ctx context.Context, conn *grpc.ClientConn, option ConsumerOption) *Consumer {
	parameters, _ := structpb.NewStruct(map[string]interface{}{})
	if option.TableRegex != "" {
		parameters.Fields[TableRegexOption] = structpb.NewStringValue(option.TableRegex)
	}
	c := &DBLogGatewayConsumer{client: pb.NewDBLogGatewayClient(conn), init: &pb.CaptureInit{
		Uri:        option.URI,
		Parameters: parameters,
	}}
	c.ctx, c.cancel = context.WithCancel(ctx)

	if option.DebounceInterval > 0 {
		return &Consumer{
			Source: c,
			Bouncer: &DebounceHandler{
				Interval: option.DebounceInterval,
				source:   c,
			},
			ctx: c.ctx,
		}
	}

	return &Consumer{Source: c, Bouncer: &NoBounceHandler{source: c}, ctx: c.ctx}
}

type ConsumerOption struct {
	URI              string
	TableRegex       string
	DebounceInterval time.Duration
}

type Consumer struct {
	Source  source.RequeueSource
	Bouncer BounceHandler
	ctx     context.Context
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
				c.Source.Commit(change.Checkpoint)
				break
			}
			c.Bouncer.Handle(ref.hdl, change.Checkpoint, Change{
				Op:  m.Change.Op,
				LSN: change.Checkpoint.LSN,
				New: makeModel(ref, m.Change.New),
				Old: makeModel(ref, m.Change.Old),
			})
		default:
			c.Source.Commit(change.Checkpoint)
		}
	}
	return c.Source.Error()
}

func makeModel(ref reflection, fields []*pb.Field) interface{} {
	if len(fields) == 0 {
		return nil
	}
	ptr := reflect.New(ref.typ)
	val := ptr.Elem()
	var err error
	for _, f := range fields {
		if f.Value == nil {
			continue
		}
		i, ok := ref.idx[f.Name]
		if !ok {
			continue
		}
		if value, ok := f.Value.(*pb.Field_Binary); ok {
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
	c.Source.Stop()
}

var ci = pgtype.NewConnInfo()
