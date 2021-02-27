package decode

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"testing"
)

func TestBytesReader(t *testing.T) {
	buf := &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, int8(math.MaxInt8))
	binary.Write(buf, binary.BigEndian, int16(math.MaxInt16))
	binary.Write(buf, binary.BigEndian, int32(math.MaxInt32))
	binary.Write(buf, binary.BigEndian, uint16(math.MaxUint16))
	binary.Write(buf, binary.BigEndian, uint32(math.MaxUint32))
	buf.Write([]byte{'B'})
	buf.Write([]byte{0})

	variable := []byte{'A', 'B', 'C', 'D', 'E', 'F', 'G', 0}

	binary.Write(buf, binary.BigEndian, int32(len(variable)))
	buf.Write(variable)

	binary.Write(buf, binary.BigEndian, int8(len(variable)))
	buf.Write(variable)

	binary.Write(buf, binary.BigEndian, int16(len(variable)))
	buf.Write(variable)

	reader := NewBytesReader(buf.Bytes())
	if v, err := reader.Int8(); v != math.MaxInt8 || err != nil {
		t.Fatalf("unexpected %v, %v", v, err)
	}
	if v, err := reader.Int16(); v != math.MaxInt16 || err != nil {
		t.Fatalf("unexpected %v, %v", v, err)
	}
	if v, err := reader.Int32(); v != math.MaxInt32 || err != nil {
		t.Fatalf("unexpected %v, %v", v, err)
	}
	if v, err := reader.Uint16(); v != uint16(math.MaxUint16) || err != nil {
		t.Fatalf("unexpected %v, %v", v, err)
	}
	if v, err := reader.Uint32(); v != uint32(math.MaxUint32) || err != nil {
		t.Fatalf("unexpected %v, %v", v, err)
	}
	if v, err := reader.Byte(); v != 'B' || err != nil {
		t.Fatalf("unexpected %v, %v", v, err)
	}
	reader.Skip(1)
	if v, err := reader.Bytes32(); !bytes.Equal(v, variable) || err != nil {
		t.Fatalf("unexpected %v, %v", v, err)
	}
	if v, err := reader.String8(); v != string(variable[:len(variable)-1]) || err != nil {
		t.Fatalf("unexpected %v, %v", v, err)
	}
	if v, err := reader.String16(); v != string(variable[:len(variable)-1]) || err != nil {
		t.Fatalf("unexpected %v, %v", v, err)
	}
	if _, err := reader.Byte(); err != io.EOF {
		t.Fatalf("unexpected %v", err)
	}
}
