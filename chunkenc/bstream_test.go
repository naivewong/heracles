package chunkenc

import (
	"encoding/binary"
	"testing"

	"github.com/naivewong/tsdb-group/testutil"
)

func TestRW(t *testing.T) {
	var b bstream

	b.writeBit(zero)
	b.writeByte(5)
	b.writeBit(one)
	b.writeBits(15, 4)
	b.writeByte(19)
	var c [binary.MaxVarintLen64]byte
	encoded := binary.PutVarint(c[:], -2229)
	for i := 0; i < encoded; i++ {
		b.writeByte(c[i])
	}
	encoded = binary.PutUvarint(c[:], 2229)
	for i := 0; i < encoded; i++ {
		b.writeByte(c[i])
	}
	b.writeBits(uint64(249421602), 28)
	b.writeBits(uint64(42876531345), 36)

	var bit bit
	var byt byte
	var x   uint64
	var uvx uint64
	var vx  int64
	var s   int
	var idx int
	var err error

	bit, err = b.ReadBitAt(idx)
	testutil.Ok(t, err)
	testutil.Equals(t, zero, bit)
	idx += 1

	byt, err = b.ReadByteAt(idx)
	testutil.Ok(t, err)
	testutil.Equals(t, byte(5), byt)
	idx += 8

	bit, err = b.ReadBitAt(idx)
	testutil.Ok(t, err)
	testutil.Equals(t, one, bit)
	idx += 1

	x, err = b.ReadBitsAt(idx, 4)
	testutil.Ok(t, err)
	testutil.Equals(t, uint64(15), x)
	idx += 4

	byt, err = b.ReadByteAt(idx)
	testutil.Ok(t, err)
	testutil.Equals(t, byte(19), byt)
	idx += 8

	vx, s, err = b.ReadVarintAt(idx)
	testutil.Ok(t, err)
	testutil.Equals(t, int64(-2229), vx)
	idx += s * 8

	uvx, s, err = b.ReadUvarintAt(idx)
	testutil.Ok(t, err)
	testutil.Equals(t, uint64(2229), uvx)
	idx += s * 8

	x, err = b.ReadBitsAt(idx, 28)
	testutil.Ok(t, err)
	testutil.Equals(t, uint64(249421602), x)
	idx += 28

	x, err = b.ReadBitsAt(idx, 36)
	testutil.Ok(t, err)
	testutil.Equals(t, uint64(42876531345), x)
}