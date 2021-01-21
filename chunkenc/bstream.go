// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// The code in this file was largely written by Damian Gryski as part of
// https://github.com/dgryski/go-tsz and published under the license below.
// It received minor modifications to suit Prometheus's needs.

// Copyright (c) 2015,2016 Damian Gryski <damian@gryski.com>
// All rights reserved.

// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:

// * Redistributions of source code must retain the above copyright notice,
// this list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
// this list of conditions and the following disclaimer in the documentation
// and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package chunkenc

import "io"

// bstream is a stream of bits.
type bstream struct {
	stream []byte // the data stream
	count  uint8  // how many bits are valid in current byte
}

func newBReader(b []byte) bstream {
	return bstream{stream: b, count: 8}
}

func (b *bstream) bytes() []byte {
	return b.stream
}

type bit bool

const (
	zero bit = false
	one  bit = true
)

func (b *bstream) writeBit(bit bit) {
	if b.count == 0 {
		b.stream = append(b.stream, 0)
		b.count = 8
	}

	i := len(b.stream) - 1

	if bit {
		b.stream[i] |= 1 << (b.count - 1)
	}

	b.count--
}

func (b *bstream) writeByte(byt byte) {
	if b.count == 0 {
		b.stream = append(b.stream, 0)
		b.count = 8
	}

	i := len(b.stream) - 1

	// fill up b.b with b.count bits from byt
	b.stream[i] |= byt >> (8 - b.count)

	b.stream = append(b.stream, 0)
	i++
	b.stream[i] = byt << b.count
}

func (b *bstream) writeBits(u uint64, nbits int) {
	u <<= (64 - uint(nbits))
	for nbits >= 8 {
		byt := byte(u >> 56)
		b.writeByte(byt)
		u <<= 8
		nbits -= 8
	}

	for nbits > 0 {
		b.writeBit((u >> 63) == 1)
		u <<= 1
		nbits--
	}
}

func (b *bstream) WriteStream(bs *bstream, num int) {
	i := 0
	for num >= 8 {
		b.writeByte(bs.stream[i])
		num -= 8
		i += 1
	}
	if num > 0 {
		j := bs.stream[i]
		for num > 0 {
			b.writeBit((j >> 7) == 1)
			j <<= 1
			num -= 1
		}
	}
}

// Pad size of 0 bits.
func (b *bstream) WritePadding(num int) {
	for num >= 8 {
		b.writeByte(0)
		num -= 8
	}
	for num > 0 {
		b.writeBit(zero)
		num -= 1
	}
}

func (b *bstream) PadToNextByte() {
	if b.count % 8 != 0 {
		b.stream = append(b.stream, 0)
		b.count = 8
	}
}

func (b *bstream) readBit() (bit, error) {
	if len(b.stream) == 0 {
		return false, io.EOF
	}

	if b.count == 0 {
		b.stream = b.stream[1:]

		if len(b.stream) == 0 {
			return false, io.EOF
		}
		b.count = 8
	}

	d := (b.stream[0] << (8 - b.count)) & 0x80
	b.count--
	return d != 0, nil
}

func (b *bstream) ReadBitAt(pos int) (bit, error) {
	idx := pos >> 3
	if idx >= len(b.stream) {
		return false, io.EOF
	}
	c := pos % 8
	return ((b.stream[idx] << uint(c)) & 0x80) != 0, nil
}

func (b *bstream) ReadByte() (byte, error) {
	return b.readByte()
}

func (b *bstream) readByte() (byte, error) {
	if len(b.stream) == 0 {
		return 0, io.EOF
	}

	if b.count == 0 {
		b.stream = b.stream[1:]

		if len(b.stream) == 0 {
			return 0, io.EOF
		}
		return b.stream[0], nil
	}

	if b.count == 8 {
		b.count = 0
		return b.stream[0], nil
	}

	byt := b.stream[0] << (8 - b.count)
	b.stream = b.stream[1:]

	if len(b.stream) == 0 {
		return 0, io.EOF
	}

	// We just advanced the stream and can assume the shift to be 0.
	byt |= b.stream[0] >> b.count

	return byt, nil
}

func (b *bstream) ReadByteAt(pos int) (byte, error) {
	idx := pos >> 3
	if idx >= len(b.stream) {
		return 0, io.EOF
	}
	c := pos % 8

	temp := b.stream[idx] << uint(c)
	idx += 1
	if c == 0 {
		return temp, nil
	} else {
		if idx >= len(b.stream) {
			return 0, io.EOF
		}
		temp |= b.stream[idx] >> uint(8 - c)
		return temp, nil
	}
}
func (b *bstream) readByteHelper(idx int, c int) (byte, int, int, error) {
	if idx >= len(b.stream) {
		return 0, 0, 0, io.EOF
	}

	temp := b.stream[idx] << uint(c)
	idx += 1
	if c == 0 {
		return temp, idx, c, nil
	} else {
		if idx >= len(b.stream) {
			return 0, 0, 0, io.EOF
		}
		temp |= b.stream[idx] >> uint(8 - c)
		return temp, idx, c, nil
	}
}

func (b *bstream) readBits(nbits int) (uint64, error) {
	var u uint64

	for nbits >= 8 {
		byt, err := b.readByte()
		if err != nil {
			return 0, err
		}

		u = (u << 8) | uint64(byt)
		nbits -= 8
	}

	if nbits == 0 {
		return u, nil
	}

	if nbits > int(b.count) {
		u = (u << uint(b.count)) | uint64((b.stream[0]<<(8-b.count))>>(8-b.count))
		nbits -= int(b.count)
		b.stream = b.stream[1:]

		if len(b.stream) == 0 {
			return 0, io.EOF
		}
		b.count = 8
	}

	u = (u << uint(nbits)) | uint64((b.stream[0]<<(8-b.count))>>(8-uint(nbits)))
	b.count -= uint8(nbits)
	return u, nil
}

func (b *bstream) ReadBitsAt(pos int, num int) (uint64, error) {
	idx := pos >> 3
	if idx >= len(b.stream) {
		return 0, io.EOF
	}
	c := pos % 8

	var result uint64
	var byt    byte
	var err    error
	for num >= 8 {
		byt, idx, c, err = b.readByteHelper(idx, c)
		if err != nil {
			return 0, err
		}
		result = (result << 8) | uint64(byt)
		num -= 8
	}

	if num == 0 {
		return result, nil
	}

	c = 8 - c
	if c < num {
		result = (result << uint(c)) | uint64(b.stream[idx] & ((1 << uint(c)) - 1))
		num -= c
		idx += 1
		if idx >= len(b.stream) {
			return 0, io.EOF
		}
		c = 8
	}
	result = (result << uint(num)) | uint64((b.stream[idx] >> uint(c - num)) & ((1 << uint(num)) - 1))
	return result, nil
}

// Return decoded value and decoded bytes number.
func (b *bstream) ReadUvarintAt(pos int) (uint64, int, error) {
	idx := pos >> 3
	if idx >= len(b.stream) {
		return 0, 0, io.EOF
	}
	c := pos % 8
	var x     uint64
	var s     uint
	var count int
	var byt   uint8
	var err   error

	for {
		byt, idx, c, err = b.readByteHelper(idx, c)
		if err != nil {
			return 0, 0, err
		}
		if byt < 0x80 {
			if count > 9 || count == 9 && byt > 1 {
				return 0, -(count + 1), nil // overflow
			}
			return x | uint64(byt)<<s, count + 1, nil
		}
		x |= uint64(byt&0x7f) << s
		s += 7
		count += 1
	}
	return 0, 0, nil // Will never reach.
}

func (b *bstream) ReadVarintAt(pos int) (int64, int, error) {
	ux, n, err := b.ReadUvarintAt(pos)
	if err != nil {
		return 0, 0, err
	}
	x := int64(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x, n, nil
}

func (b *bstream) GetStream() []byte {
	return b.stream
}

func (b *bstream) Size() int {
	return len(b.stream)
}

func (b *bstream) WritePos() int {
	if b.count == 0 {
		return len(b.stream) << 3
	}
	return ((len(b.stream) - 1) << 3) + 8 - int(b.count)
}
