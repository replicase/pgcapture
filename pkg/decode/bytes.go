package decode

import (
	"bytes"
	"encoding/binary"
	"io"
)

func NewBytesReader(data []byte) *BytesReader {
	return &BytesReader{data: data}
}

type BytesReader struct {
	data []byte
	off  int
}

func (b *BytesReader) Skip(n int) {
	b.off += n
	return
}

func (b *BytesReader) Byte() (v byte, err error) {
	if b.off >= len(b.data) {
		return 0, io.EOF
	}
	v = b.data[b.off]
	b.off++
	return
}

func (b *BytesReader) Uint32() (v uint32, err error) {
	end := b.off + 4
	if end > len(b.data) {
		return 0, io.EOF
	}
	v = binary.BigEndian.Uint32(b.data[b.off:end])
	b.off = end
	return
}

func (b *BytesReader) Uint64() (v uint64, err error) {
	end := b.off + 8
	if end > len(b.data) {
		return 0, io.EOF
	}
	v = binary.BigEndian.Uint64(b.data[b.off:end])
	b.off = end
	return
}

func (b *BytesReader) Uint16() (v uint16, err error) {
	end := b.off + 2
	if end > len(b.data) {
		return 0, io.EOF
	}
	v = binary.BigEndian.Uint16(b.data[b.off:end])
	b.off = end
	return
}

func (b *BytesReader) Int32() (v int, err error) {
	uv, err := b.Uint32()
	return int(uv), err
}

func (b *BytesReader) Int64() (v int, err error) {
	uv, err := b.Uint64()
	return int(uv), err
}

func (b *BytesReader) Int16() (v int, err error) {
	uv, err := b.Uint16()
	return int(uv), err
}

func (b *BytesReader) Int8() (v int, err error) {
	uv, err := b.Byte()
	return int(uv), err
}

func (b *BytesReader) IntEndIdx() (v int, err error) {
	idx := bytes.IndexByte(b.data[b.off:], byte(0))
	if idx == -1 {
		return 0, io.EOF
	}
	return idx, nil
}

func (b *BytesReader) stringN(n int) (v string, err error) {
	end := b.off + n
	if end > len(b.data) {
		return "", io.EOF
	}
	v = string(b.data[b.off : end-1])
	b.off = end
	return
}

func (b *BytesReader) String8() (v string, err error) {
	n, err := b.Int8()
	if err != nil {
		return "", err
	}
	return b.stringN(n)
}

func (b *BytesReader) String16() (v string, err error) {
	n, err := b.Int16()
	if err != nil {
		return "", err
	}
	return b.stringN(n)
}

func (b *BytesReader) Bytes32() (v []byte, err error) {
	n, err := b.Int32()
	if err != nil {
		return nil, err
	}
	end := b.off + n
	if end > len(b.data) {
		return nil, io.EOF
	}
	v = b.data[b.off:end]
	b.off = end
	return
}

func (b *BytesReader) StringEnd() (v string, err error) {
	idx, err := b.IntEndIdx()
	if err != nil {
		return "", err
	}

	return b.stringN(idx + 1)
}
