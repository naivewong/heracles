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

package chunkenc

import (
	"fmt"
	"hash"
	"sync"

	"github.com/pkg/errors"
)

// Encoding is the identifier for a chunk encoding.
type Encoding uint8

func (e Encoding) String() string {
	switch e {
	case EncNone:
		return "none"
	case EncXOR:
		return "XOR"
	case EncGMC1:
		return "GMC1"
	case EncGMC2:
		return "GMC2"
	case EncGDC1:
		return "GDC1"
	}
	return "<unknown>"
}

// The different available chunk encodings.
const (
	EncNone Encoding = iota
	EncXOR
	EncGMC1
	EncGMC2
	EncGDC1
	EncGHC1
)

// Chunk holds a sequence of sample pairs that can be iterated over and appended to.
type Chunk interface {
	Bytes() []byte
	Encoding() Encoding
	Appender() (Appender, error)
	Iterator() Iterator
	IteratorGroup(int64, int) Iterator
	NumSamples() int

	// NOTE(Alec), this is to get the GMC from SafeChunk in head.go.
	Chunk() Chunk
}

// Appender adds sample pairs to a chunk.
type Appender interface {
	Append(int64, float64)
	AppendGroup(int64, []float64)
}

// Iterator is a simple iterator that can only get the next value.
type Iterator interface {
	At() (int64, float64)
	Err() error
	Next() bool
	Seek(int64) bool
}

// NewNopIterator returns a new chunk iterator that does not hold any data.
func NewNopIterator() Iterator {
	return nopIterator{}
}

type nopIterator struct{}

func (nopIterator) At() (int64, float64) { return 0, 0 }
func (nopIterator) Next() bool           { return false }
func (nopIterator) Seek(x int64) bool    { return false }
func (nopIterator) Err() error           { return nil }

func NewNopAppender() Appender {
	return nopAppender{}
}

type nopAppender struct{}

func (nopAppender) Append(t int64, v float64)        {}
func (nopAppender) AppendGroup(t int64, v []float64) {}

// Pool is used to create and reuse chunk references to avoid allocations.
type Pool interface {
	Put(Chunk) error
	Get(e Encoding, b []byte) (Chunk, error)
}

// pool is a memory pool of chunk objects.
type pool struct {
	// xor sync.Pool
	gdc sync.Pool
	// gmc sync.Pool
}

// NewPool returns a new pool.
func NewPool() Pool {
	return &pool{
		// xor: sync.Pool{
		// 	New: func() interface{} {
		// 		return &XORChunk{b: bstream{}}
		// 	},
		// },
		gdc: sync.Pool{
			New: func() interface{} {
				return &GroupDiskChunk1{b: &bstream{}}
			},
		},
	}
}

func (p *pool) Get(e Encoding, b []byte) (Chunk, error) {
	switch e {
	// case EncXOR:
	// 	c := p.xor.Get().(*XORChunk)
	// 	c.b.stream = b
	// 	c.b.count = 0
	// 	return c, nil
	case EncGDC1:
		c := p.gdc.Get().(*GroupDiskChunk1)
		c.b.stream = b
		x, n, _ := c.b.ReadUvarintAt(0)
		c.numTimestamp = int(x)
		x, _, _ = c.b.ReadUvarintAt(n << 3)
		c.numSeries = int(x)
		c.segments = DefaultMedianSegment
		c.alignments = DefaultTupleSize
		return c, nil
	}
	return nil, errors.Errorf("invalid encoding %q", e)
}

func (p *pool) Put(c Chunk) error {
	switch c.Encoding() {
	// case EncXOR:
	// 	xc, ok := c.(*XORChunk)
	// 	// This may happen often with wrapped chunks. Nothing we can really do about
	// 	// it but returning an error would cause a lot of allocations again. Thus,
	// 	// we just skip it.
	// 	if !ok {
	// 		return nil
	// 	}
	// 	xc.b.stream = nil
	// 	xc.b.count = 0
	// 	p.xor.Put(c)
	case EncGDC1:
		xc, ok := c.(*GroupDiskChunk1)
		if !ok {
			return nil
		}
		xc.b.stream = nil
		p.gdc.Put(c)
	default:
		return errors.Errorf("invalid encoding %q", c.Encoding())
	}
	return nil
}

// FromData returns a chunk from a byte slice of chunk data.
// This is there so that users of the library can easily create chunks from
// bytes.
func FromData(e Encoding, d []byte) (Chunk, error) {
	switch e {
	case EncXOR:
		return &XORChunk{b: bstream{count: 0, stream: d}}, nil
	}
	return nil, fmt.Errorf("unknown chunk encoding: %d", e)
}

// Meta holds information about a chunk of data.
type Meta struct {
	// Ref and Chunk hold either a reference that can be used to retrieve
	// chunk data or the data itself.
	// Generally, only one of them is set.
	Ref             uint64
	SeriesRef       uint64
	LogicalGroupRef uint64
	Chunk           Chunk

	MinTime, MaxTime int64 // time range the data covers
}

// writeHash writes the chunk encoding and raw data into the provided hash.
func (cm *Meta) WriteHash(h hash.Hash) error {
	// if _, err := h.Write([]byte{byte(cm.Chunk.Encoding())}); err != nil {
	// 	return err
	// }
	if _, err := h.Write(cm.Chunk.Bytes()); err != nil {
		return err
	}
	return nil
}

// OverlapsClosedInterval Returns true if the chunk overlaps [mint, maxt].
func (cm *Meta) OverlapsClosedInterval(mint, maxt int64) bool {
	// The chunk itself is a closed interval [cm.MinTime, cm.MaxTime].
	return cm.MinTime <= maxt && mint <= cm.MaxTime
}
