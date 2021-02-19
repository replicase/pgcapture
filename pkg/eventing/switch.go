package eventing

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/jackc/pgtype"
)

type SwitchHandler struct {
	Model   interface{}
	Handler func(model interface{}, deleted bool)
}

type SwitchTable struct {
	namespace string
	fieldIdx  map[string]fieldIdx
}

type fieldIdx struct {
	idx map[string]int
	typ reflect.Type
	hdl func(model interface{}, deleted bool)
}

var decoderType = reflect.TypeOf((*pgtype.BinaryDecoder)(nil)).Elem()

func MakeSwitchTable(namespace string, handlers map[string]SwitchHandler) (*SwitchTable, error) {
	fdx := make(map[string]fieldIdx, len(handlers))
	for table, h := range handlers {
		typ := reflect.TypeOf(h.Model)
		if typ.Kind() != reflect.Ptr || typ.Elem().Kind() != reflect.Struct {
			return nil, errors.New("the field Model of SwitchHandler should be a pointer of struct")
		}
		typ = typ.Elem()
		fdx[table] = fieldIdx{idx: make(map[string]int, typ.NumField()), typ: typ, hdl: h.Handler}
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
				fdx[table].idx[n[0]] = i
			}
		}
	}
	return &SwitchTable{namespace: namespace, fieldIdx: fdx}, nil
}
