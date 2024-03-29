package pgcapture

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/replicase/pgcapture/pkg/cursor"
	"github.com/replicase/pgcapture/pkg/pb"
)

type Model interface {
	TableName() (schema, table string)
}

type Change struct {
	Op         pb.Change_Operation
	Checkpoint cursor.Checkpoint
	New        interface{}
	Old        interface{}
}

type ModelHandlerFunc func(change Change) error
type ModelAsyncHandlerFunc func(change Change, done func(err error))
type ModelHandlers map[Model]ModelHandlerFunc
type ModelAsyncHandlers map[Model]ModelAsyncHandlerFunc

func toAsyncHandlerFunc(fn ModelHandlerFunc) ModelAsyncHandlerFunc {
	return func(change Change, done func(err error)) {
		done(fn(change))
	}
}

func reflectModel(model Model) (ref reflection, err error) {
	typ := reflect.TypeOf(model)
	if typ.Kind() != reflect.Ptr || typ.Elem().Kind() != reflect.Struct {
		return ref, errors.New("the field Model of SwitchHandler should be a pointer of struct")
	}
	typ = typ.Elem()
	ref = reflection{idx: make(map[string]int, typ.NumField()), typ: typ}
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if tag, ok := f.Tag.Lookup("pg"); ok {
			if n := strings.Split(tag, ","); len(n) > 0 && n[0] != "" {
				ref.idx[n[0]] = i
			}
		}
	}
	for k := range ref.idx {
		if k != "" {
			return ref, nil
		}
	}
	return ref, fmt.Errorf("at least one field of %s should should have a valid pg tag", typ.Elem())
}

func ModelName(namespace, table string) string {
	if namespace == "" {
		return "public." + table
	}
	return namespace + "." + table
}

type reflection struct {
	idx map[string]int
	typ reflect.Type
	hdl ModelAsyncHandlerFunc
}
