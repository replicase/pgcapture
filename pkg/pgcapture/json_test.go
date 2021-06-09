package pgcapture

import (
	"bytes"
	"github.com/jackc/pgtype"
	"testing"
)

func TestMarshalJSON(t *testing.T) {
	m := &m1{}
	m.F1.Set("f1")
	m.F2.Set("")
	m.F3.Set(nil)
	m.F4.Set(nil)
	m.F5.Set("")
	m.F9 = make([]string, 0)
	m.F10 = []string{"1"}
	bs, err := MarshalJSON(m)
	if err != nil {
		t.Fatalf("unexpected err %v", err)
	}
	if !bytes.Equal(bs, []byte(`{"f1":"f1","f4":null,"F5":"","F10":["1"]}`)) {
		t.Fatalf("unexpected json %v", string(bs))
	}
	m.F6.Get()
}

type m1 struct {
	F1 pgtype.Text `json:"f1"`
	F2 pgtype.Text `json:"f2,omitempty"`
	F3 pgtype.Text `json:",omitempty"`
	F4 pgtype.Text `json:"f4"`
	F5 pgtype.Text
	F6 pgtype.Text

	F7  string   `json:",omitempty"`
	F8  []string `json:",omitempty"`
	F9  []string `json:"f9,omitempty"`
	F10 []string
}

func (m *m1) TableName() (schema, table string) {
	return "", ""
}
