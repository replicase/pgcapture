package eventing

import (
	"errors"
	"reflect"
	"sync/atomic"

	"github.com/jackc/pgtype"
	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/rueian/pgcapture/pkg/source"
)

func NewConsumer(s source.TxSource) *Consumer {
	return &Consumer{source: s}
}

type Consumer struct {
	source source.TxSource
	err    error
}

func (c *Consumer) Error() error {
	return c.err
}

func (c *Consumer) Consume() (chan *TxEvent, error) {
	changes, err := c.source.Capture(source.Checkpoint{})
	if err != nil {
		return nil, err
	}

	events := make(chan *TxEvent)
	go func() {
		defer close(events)
		var tx *TxEvent
		for change := range changes {
			switch m := change.Message.Type.(type) {
			case *pb.Message_Begin:
				if tx == nil {
					tx = &TxEvent{c: c, changes: make(chan *pb.Change)}
					events <- tx
					continue
				}
			case *pb.Message_Change:
				if tx != nil {
					tx.changes <- m.Change
					continue
				}
			case *pb.Message_Commit:
				if tx != nil {
					atomic.StoreUint64(&tx.commit, change.Checkpoint.LSN)
					close(tx.changes)
					tx = nil
					continue
				}
			}
			c.err = errors.New("receive incomplete transaction")
			return
		}
	}()
	return events, nil
}

func (c *Consumer) Stop() {
	c.source.Stop()
}

type TxEvent struct {
	c       *Consumer
	commit  uint64
	changes chan *pb.Change
}

func (e *TxEvent) NextChange() (c *pb.Change, more bool) {
	c, more = <-e.changes
	return
}

func (e *TxEvent) Ack() {
	for range e.changes {
	}
	e.c.source.Commit(source.Checkpoint{LSN: atomic.LoadUint64(&e.commit)})
}

func (e *TxEvent) Switch(st *SwitchTable) error {
	var fields []*pb.Field
	for c := range e.changes {
		if st.namespace != c.Namespace {
			continue
		}
		sh, ok := st.fieldIdx[c.Table]
		if !ok {
			continue
		}
		deleted := c.Op == pb.Change_DELETE
		if deleted {
			fields = c.OldTuple
		} else {
			fields = c.NewTuple
		}
		v := reflect.New(sh.typ)
		for _, f := range fields {
			i, ok := sh.idx[f.Name]
			if !ok {
				continue
			}
			if err := v.Elem().Field(i).Addr().Interface().(pgtype.BinaryDecoder).DecodeBinary(ci, f.Datum); err != nil {
				return err
			}
		}
		sh.hdl(v.Interface(), deleted)
	}
	return nil
}

var ci = pgtype.NewConnInfo()
