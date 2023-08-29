package pgcapture

import (
	"bytes"
	"testing"

	pgtypeV4 "github.com/jackc/pgtype"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestMarshalJSON(t *testing.T) {
	testCases := []struct {
		input  Model
		expect []byte
	}{
		{
			input: &m1{
				F1:  pgtypeV4.Text{String: "f1", Status: pgtypeV4.Present},
				F2:  pgtypeV4.Text{String: "", Status: pgtypeV4.Present},
				F3:  pgtypeV4.Text{String: "", Status: pgtypeV4.Null},
				F4:  pgtypeV4.Text{String: "", Status: pgtypeV4.Null},
				F5:  pgtypeV4.Text{String: "", Status: pgtypeV4.Present},
				F9:  make([]string, 0),
				F10: []string{"1"},
			},
			expect: []byte(`{"f1":"f1","f4":null,"F5":"","F10":["1"]}`),
		},
		{
			input: &m2{
				F1:  pgtype.Text{String: "f1", Valid: true},
				F2:  pgtype.Text{String: "", Valid: true},
				F3:  pgtype.Text{String: "", Valid: false},
				F4:  pgtype.Text{String: "", Valid: false},
				F5:  pgtype.Text{String: "", Valid: true},
				F9:  make([]string, 0),
				F10: []string{"1"},
			},
			expect: []byte(`{"f1":"f1","f4":null,"F5":"","F6":null,"F10":["1"]}`),
		},
	}

	for _, tc := range testCases {
		bs, err := MarshalJSON(tc.input)
		if err != nil {
			t.Fatalf("unexpected err %v", err)
		}
		if !bytes.Equal(bs, tc.expect) {
			t.Fatalf("unexpected json %v", string(bs))
		}
	}
}

type m1 struct {
	F1 pgtypeV4.Text `json:"f1"`
	F2 pgtypeV4.Text `json:"f2,omitempty"`
	F3 pgtypeV4.Text `json:",omitempty"`
	F4 pgtypeV4.Text `json:"f4"`
	F5 pgtypeV4.Text
	F6 pgtypeV4.Text

	F7  string   `json:",omitempty"`
	F8  []string `json:",omitempty"`
	F9  []string `json:"f9,omitempty"`
	F10 []string
}

func (m *m1) TableName() (schema, table string) {
	return "", ""
}

type m2 struct {
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

func (m *m2) TableName() (schema, table string) {
	return "", ""
}
