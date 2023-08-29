package pgcapture

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"reflect"
	"strings"
	"sync"

	pgtypeV4 "github.com/jackc/pgtype"
)

var bufPool sync.Pool
var fieldsCache sync.Map

type fieldOption struct {
	name      string
	omitempty bool
}

func MarshalJSON(m Model) ([]byte, error) {
	var buf *bytes.Buffer
	if v := bufPool.Get(); v != nil {
		buf = v.(*bytes.Buffer)
	} else {
		buf = bytes.NewBuffer(nil)
	}
	defer func() {
		buf.Reset()
		bufPool.Put(buf)
	}()

	val := reflect.ValueOf(m)
	ele := val.Elem()
	typ := ele.Type()

	if val.IsNil() {
		buf.WriteString("null")
		return buf.Bytes(), nil
	}

	var names []fieldOption
	if v, ok := fieldsCache.Load(typ); ok {
		names = v.([]fieldOption)
	} else {
		names = make([]fieldOption, typ.NumField())
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			if tag, ok := field.Tag.Lookup("json"); ok {
				parts := strings.Split(tag, ",")
				opt := fieldOption{name: parts[0]}
				if len(parts) != 1 {
					opt.omitempty = strings.Contains(parts[1], "omitempty")
				}
				if opt.omitempty && opt.name == "" {
					opt.name = field.Name
				}
				names[i] = opt
			} else {
				names[i] = fieldOption{name: field.Name}
			}
		}
		fieldsCache.Store(typ, names)
	}

	buf.WriteString("{")

	count := 0
	for i, opt := range names {
		f := ele.Field(i)
		if !f.CanInterface() || opt.name == "-" {
			continue
		}
		face := f.Addr().Interface()
		if opt.omitempty {
			if valuer, ok := face.(pgtypeV4.Value); ok {
				if v := valuer.Get(); v == nil || isEmptyValue(reflect.ValueOf(v)) {
					continue
				}
			} else if valuer, ok := face.(driver.Valuer); ok {
				if v, err := valuer.Value(); err != nil {
					return nil, err
				} else if v == nil || isEmptyValue(reflect.ValueOf(v)) {
					continue
				}
			} else {
				if (f.Kind() == reflect.Ptr && f.IsNil()) || isEmptyValue(f) {
					continue
				}
			}
		}
		bs, err := json.Marshal(face)
		if err != nil {
			if strings.Contains(err.Error(), "cannot encode status undefined") {
				continue
			}
			return nil, err
		}
		if count > 0 {
			buf.WriteString(",")
		}
		buf.WriteString("\"")
		buf.WriteString(opt.name)
		buf.WriteString("\":")
		buf.Write(bs)
		count++
	}
	buf.WriteString("}")
	return buf.Bytes(), nil
}

func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}
