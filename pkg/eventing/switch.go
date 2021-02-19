package eventing

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/jackc/pgtype"
)

type Model interface {
	TableName() (namespace, table string)
}

type switchHandler func(model interface{}, deleted bool)
type ModelHandlers map[Model]switchHandler
type ModelSwitch map[string]register

func NewModelSwitch(handlers ModelHandlers) (ModelSwitch, error) {
	ms := make(map[string]register, len(handlers))
	for model, h := range handlers {
		typ := reflect.TypeOf(model)
		if typ.Kind() != reflect.Ptr || typ.Elem().Kind() != reflect.Struct {
			return nil, errors.New("the field Model of SwitchHandler should be a pointer of struct")
		}
		typ = typ.Elem()
		key := modelKey(model.TableName())
		ms[key] = register{idx: make(map[string]int, typ.NumField()), typ: typ, hdl: h}
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			if !reflect.PtrTo(f.Type).Implements(decoderType) {
				return nil, fmt.Errorf("the field %s of %s should be a pgtype.BinaryDecoder", f.Name, typ.Elem())
			}
			tag, ok := f.Tag.Lookup("pg")
			if !ok {
				return nil, fmt.Errorf("the field %s of %s should should have a pg tag", f.Name, typ.Elem())
			}
			if n := strings.Split(tag, ","); len(n) > 0 && n[0] != "" {
				ms[key].idx[n[0]] = i
			}
		}
	}
	return ms, nil
}

func modelKey(namespace, table string) string {
	if namespace == "" {
		return "public." + table
	}
	return namespace + "." + table
}

type register struct {
	idx map[string]int
	typ reflect.Type
	hdl switchHandler
}

var decoderType = reflect.TypeOf((*pgtype.BinaryDecoder)(nil)).Elem()
