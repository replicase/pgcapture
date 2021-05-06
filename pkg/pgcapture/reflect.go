package pgcapture

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/jackc/pgtype"
	"github.com/rueian/pgcapture/pkg/pb"
)

type Model interface {
	TableName() (schema, table string)
}

type Change struct {
	Op  pb.Change_Operation
	LSN uint64
	New interface{}
	Old interface{}
}

type ModelHandlerFunc func(change Change) error
type ModelHandlers map[Model]ModelHandlerFunc

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
			if !reflect.PtrTo(f.Type).Implements(decoderType) {
				return ref, fmt.Errorf("the field %s of %s should be a pgtype.BinaryDecoder", f.Name, typ.Elem())
			}
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
	hdl ModelHandlerFunc
}

var decoderType = reflect.TypeOf((*pgtype.BinaryDecoder)(nil)).Elem()
