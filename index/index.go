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

package index

import (
	// "fmt"
	"bufio"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/naivewong/tsdb-group/chunkenc"
	"github.com/naivewong/tsdb-group/encoding"
	tsdb_errors "github.com/naivewong/tsdb-group/errors"
	"github.com/naivewong/tsdb-group/fileutil"
	"github.com/naivewong/tsdb-group/labels"
)

const (
	// MagicIndex 4 bytes at the head of an index file.
	MagicIndex = 0xBAAAD700
	// HeaderLen represents number of bytes reserved of index for header.
	HeaderLen = 5

	// FormatV1 represents 1 version of index.
	FormatV1 = 1
	// FormatV2 represents 2 version of index.
	FormatV2 = 2
	// FormatGroup represent group version of index.
	FormatGroup = 3

	labelNameSeperator = "\xff"

	indexFilename = "index"
)

type indexWriterSeries struct {
	labels labels.Labels
	chunks []chunkenc.Meta // series file offset of chunks
}

type indexWriterSeriesSlice []*indexWriterSeries

func (s indexWriterSeriesSlice) Len() int      { return len(s) }
func (s indexWriterSeriesSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s indexWriterSeriesSlice) Less(i, j int) bool {
	return labels.Compare(s[i].labels, s[j].labels) < 0
}

type indexWriterStage uint8

const (
	idxStageNone indexWriterStage = iota
	idxStageSymbols
	idxStageSeries
	idxStageLabelIndex
	idxStagePostings
	idxStageGroupPostings
	idxStageDone
)

func (s indexWriterStage) String() string {
	switch s {
	case idxStageNone:
		return "none"
	case idxStageSymbols:
		return "symbols"
	case idxStageSeries:
		return "series"
	case idxStageLabelIndex:
		return "label index"
	case idxStagePostings:
		return "postings"
	case idxStageGroupPostings:
		return "group postings"
	case idxStageDone:
		return "done"
	}
	return "<unknown>"
}

// The table gets initialized with sync.Once but may still cause a race
// with any other use of the crc32 package anywhere. Thus we initialize it
// before.
var castagnoliTable *crc32.Table

func init() {
	castagnoliTable = crc32.MakeTable(crc32.Castagnoli)
}

// newCRC32 initializes a CRC32 hash with a preconfigured polynomial, so the
// polynomial may be easily changed in one location at a later time, if necessary.
func newCRC32() hash.Hash32 {
	return crc32.New(castagnoliTable)
}

// Writer implements the IndexWriter interface for the standard
// serialization format.
type Writer struct {
	f    *os.File
	fbuf *bufio.Writer
	pos  uint64

	toc   TOC
	stage indexWriterStage

	// Reusable memory.
	buf1    encoding.Encbuf
	buf2    encoding.Encbuf
	uint32s []uint32

	symbols       map[string]uint32 // symbol offsets
	seriesOffsets map[uint64]uint64 // offsets of series
	labelIndexes  []hashEntry       // label index offsets
	postings      []hashEntry       // postings lists offsets
	groupPostings []uint64          // group postings offsets

	// Hold last series to validate that clients insert new series in order.
	lastSeries labels.Labels

	crc32 hash.Hash

	Version int
}

// TOC represents index Table Of Content that states where each section of index starts.
type TOC struct {
	Symbols            uint64
	Series             uint64
	LabelIndices       uint64
	LabelIndicesTable  uint64
	Postings           uint64
	PostingsTable      uint64
	GroupPostings      uint64
	GroupPostingsTable uint64
}

// NewTOCFromByteSlice return parsed TOC from given index byte slice.
func NewTOCFromByteSlice(bs ByteSlice) (*TOC, error) {
	if bs.Len() < indexTOCLen {
		return nil, encoding.ErrInvalidSize
	}
	b := bs.Range(bs.Len()-indexTOCLen, bs.Len())

	expCRC := binary.BigEndian.Uint32(b[len(b)-4:])
	d := encoding.Decbuf{B: b[:len(b)-4]}

	if d.Crc32(castagnoliTable) != expCRC {
		return nil, errors.Wrap(encoding.ErrInvalidChecksum, "read TOC")
	}

	if err := d.Err(); err != nil {
		return nil, err
	}

	return &TOC{
		Symbols:            d.Be64(),
		Series:             d.Be64(),
		LabelIndices:       d.Be64(),
		LabelIndicesTable:  d.Be64(),
		Postings:           d.Be64(),
		PostingsTable:      d.Be64(),
		GroupPostings:      d.Be64(),
		GroupPostingsTable: d.Be64(),
	}, nil
}

// NewWriter returns a new Writer to the given filename. It serializes data in format version 2.
func NewWriter(fn string) (*Writer, error) {
	dir := filepath.Dir(fn)

	df, err := fileutil.OpenDir(dir)
	if err != nil {
		return nil, err
	}
	defer df.Close() // Close for platform windows.

	if err := os.RemoveAll(fn); err != nil {
		return nil, errors.Wrap(err, "remove any existing index at path")
	}

	f, err := os.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	if err := df.Sync(); err != nil {
		return nil, errors.Wrap(err, "sync dir")
	}

	iw := &Writer{
		f:     f,
		fbuf:  bufio.NewWriterSize(f, 1<<22),
		pos:   0,
		stage: idxStageNone,

		// Reusable memory.
		buf1:    encoding.Encbuf{B: make([]byte, 0, 1<<22)},
		buf2:    encoding.Encbuf{B: make([]byte, 0, 1<<22)},
		uint32s: make([]uint32, 0, 1<<15),

		// Caches.
		symbols:       make(map[string]uint32, 1<<13),
		seriesOffsets: make(map[uint64]uint64, 1<<16),
		crc32:         newCRC32(),
	}
	if err := iw.writeMeta(); err != nil {
		return nil, err
	}
	return iw, nil
}

func (w *Writer) write(bufs ...[]byte) error {
	for _, b := range bufs {
		n, err := w.fbuf.Write(b)
		w.pos += uint64(n)
		if err != nil {
			return err
		}
		// For now the index file must not grow beyond 64GiB. Some of the fixed-sized
		// offset references in v1 are only 4 bytes large.
		// Once we move to compressed/varint representations in those areas, this limitation
		// can be lifted.
		if w.pos > 16*math.MaxUint32 {
			return errors.Errorf("exceeding max size of 64GiB")
		}
	}
	return nil
}

// addPadding adds zero byte padding until the file size is a multiple size.
func (w *Writer) addPadding(size int) error {
	p := w.pos % uint64(size)
	if p == 0 {
		return nil
	}
	p = uint64(size) - p
	return errors.Wrap(w.write(make([]byte, p)), "add padding")
}

// ensureStage handles transitions between write stages and ensures that IndexWriter
// methods are called in an order valid for the implementation.
func (w *Writer) ensureStage(s indexWriterStage) error {
	if w.stage == s {
		return nil
	}
	if w.stage > s {
		return errors.Errorf("invalid stage %q, currently at %q", s, w.stage)
	}

	// Mark start of sections in table of contents.
	switch s {
	case idxStageSymbols:
		w.toc.Symbols = w.pos
	case idxStageSeries:
		w.toc.Series = w.pos

	case idxStageLabelIndex:
		w.toc.LabelIndices = w.pos

	case idxStagePostings:
		w.toc.Postings = w.pos

	case idxStageGroupPostings:
		w.toc.GroupPostings = w.pos

	case idxStageDone:
		w.toc.LabelIndicesTable = w.pos
		if err := w.writeOffsetTable(w.labelIndexes); err != nil {
			return err
		}
		w.toc.PostingsTable = w.pos
		if err := w.writeOffsetTable(w.postings); err != nil {
			return err
		}
		w.toc.GroupPostingsTable = w.pos
		if err := w.writeGroupPostingsTable(); err != nil {
			return err
		}
		if err := w.writeTOC(); err != nil {
			return err
		}
	}

	w.stage = s
	return nil
}

func (w *Writer) writeMeta() error {
	w.buf1.Reset()
	w.buf1.PutBE32(MagicIndex)
	w.buf1.PutByte(FormatGroup)

	return w.write(w.buf1.Get())
}

// AddSeries adds the series one at a time along with its chunks.
// ┌────────────────────────────────────────────────────────────────┐
// │ len <uvarint>                                                  │
// ├────────────────────────────────────────────────────────────────┤
// │ group_ref(logical) <8b>                                        │
// ├────────────────────────────────────────────────────────────────┤
// │ ┌────────────────────────────────────────────────────────────┐ │
// │ │           labels count <uvarint64>                         │ │
// │ ├────────────────────────────────────────────────────────────┤ │
// │ │    ┌────────────────────────────────────────────┐          │ │
// │ │    │ ref(l_i.name) <uvarint32>                  │          │ │
// │ │    ├────────────────────────────────────────────┤          │ │
// │ │    │ ref(l_i.value) <uvarint32>                 │          │ │
// │ │    └────────────────────────────────────────────┘          │ │
// │ │                   ...                                      │ │
// │ ├────────────────────────────────────────────────────────────┤ │
// │ │           chunks count <uvarint>                           │ │
// │ ├────────────────────────────────────────────────────────────┤ │
// │ │    ┌────────────────────────────────────────────┐          │ │
// │ │    │ min_time <varint>                          │          │ │
// │ │    ├────────────────────────────────────────────┤          │ │
// │ │    │ max_time - min_time <uvarint>              │          │ │
// │ │    ├────────────────────────────────────────────┤          │ │
// │ │    │ Group start offset <uvarint>               │          │ │
// │ │    ├────────────────────────────────────────────┤          │ │
// │ │    │ Series relative offset <uvarint>           │          │ │
// │ │    └────────────────────────────────────────────┘          │ │
// │ │    ┌────────────────────────────────────────────┐          │ │
// │ │    │ c_0.maxt - c_0.mint <uvarint>              │          │ │
// │ │    ├────────────────────────────────────────────┤          │ │
// │ │    │ c_i.mint - c_i-1.maxt <uvarint>            │          │ │
// │ │    ├────────────────────────────────────────────┤          │ │
// │ │    │ ref(c_i.gr) - ref(c_i-1.gr) <varint>       │          │ │
// │ │    ├────────────────────────────────────────────┤          │ │
// │ │    │ ref(c_i.sr) - ref(c_i-1.sr) <varint>       │          │ │
// │ │    └────────────────────────────────────────────┘          │ │
// │ │                   ...                                      │ │
// │ └────────────────────────────────────────────────────────────┘ │
// ├────────────────────────────────────────────────────────────────┤
// │ CRC32 <4b>                                                     │
// └────────────────────────────────────────────────────────────────┘
//  
// Adrress series entry by 4 bytes reference 
// Align to 16 bytes  
// NOTICE: The ref here is just a temporary ref assigned as monotonically increasing id in memory.
func (w *Writer) AddSeries(ref uint64, lset labels.Labels, chunks ...chunkenc.Meta) error {
	if err := w.ensureStage(idxStageSeries); err != nil {
		return err
	}
	if labels.Compare(lset, w.lastSeries) <= 0 {
		return errors.Errorf("out-of-order series added with label set %q", lset)
	}

	if _, ok := w.seriesOffsets[ref]; ok {
		return errors.Errorf("series with reference %d already added", ref)
	}
	// We add padding to 16 bytes to increase the addressable space we get through 4 byte
	// series references.
	if err := w.addPadding(16); err != nil {
		return errors.Errorf("failed to write padding bytes: %v", err)
	}

	if w.pos%16 != 0 {
		return errors.Errorf("series write not 16-byte aligned at %d", w.pos)
	}
	w.seriesOffsets[ref] = w.pos / 16

	w.buf2.Reset()
	w.buf2.PutUvarint(len(lset)) // Number of label pairs.

	for _, l := range lset {
		// here we have an index for the symbol file if v2, otherwise it's an offset
		index, ok := w.symbols[l.Name]
		if !ok {
			return errors.Errorf("symbol entry for %q does not exist", l.Name)
		}
		w.buf2.PutUvarint32(index)

		index, ok = w.symbols[l.Value]
		if !ok {
			return errors.Errorf("symbol entry for %q does not exist", l.Value)
		}
		w.buf2.PutUvarint32(index)
	}

	w.buf2.PutUvarint(len(chunks)) // Number of chunks.

	if len(chunks) > 0 {
		c := chunks[0]
		w.buf2.PutVarint64(c.MinTime)
		w.buf2.PutUvarint64(uint64(c.MaxTime - c.MinTime))
		w.buf2.PutUvarint64(c.Ref)
		w.buf2.PutUvarint64(c.SeriesRef)
		t0 := c.MaxTime
		ref0 := int64(c.Ref)
		ref1 := int64(c.SeriesRef)

		for _, c := range chunks[1:] {
			w.buf2.PutUvarint64(uint64(c.MinTime - t0))
			w.buf2.PutUvarint64(uint64(c.MaxTime - c.MinTime))
			t0 = c.MaxTime

			w.buf2.PutVarint64(int64(c.Ref) - ref0)
			w.buf2.PutVarint64(int64(c.SeriesRef) - ref1)
			ref0 = int64(c.Ref)
			ref1 = int64(c.SeriesRef)
		}
	}

	w.buf1.Reset()
	w.buf1.PutUvarint(w.buf2.Len()) // Len in the beginning.
	w.buf1.PutBE64(0)               // Logical group ref(will be updated in write_group_postings.

	// NOTE(Alec), the logical group ref is not counted into the crc32.
	w.buf2.PutHash(w.crc32)

	if err := w.write(w.buf1.Get(), w.buf2.Get()); err != nil {
		return errors.Wrap(err, "write series data")
	}

	w.lastSeries = append(w.lastSeries[:0], lset...)

	return nil
}

// ┌────────────────────┬─────────────────────┐
// │ len <4b>           │ #symbols <4b>       │
// ├────────────────────┴─────────────────────┤
// │ ┌──────────────────────┬───────────────┐ │
// │ │ len(str_1) <uvarint> │ str_1 <bytes> │ │
// │ ├──────────────────────┴───────────────┤ │
// │ │                . . .                 │ │
// │ ├──────────────────────┬───────────────┤ │
// │ │ len(str_n) <uvarint> │ str_n <bytes> │ │
// │ └──────────────────────┴───────────────┘ │
// ├──────────────────────────────────────────┤
// │ CRC32 <4b>                               │
// └──────────────────────────────────────────┘
//
// The label pairs are sorted.
func (w *Writer) AddSymbols(sym map[string]struct{}) error {
	if err := w.ensureStage(idxStageSymbols); err != nil {
		return err
	}
	// Generate sorted list of strings we will store as reference table.
	symbols := make([]string, 0, len(sym))

	for s := range sym {
		symbols = append(symbols, s)
	}
	sort.Strings(symbols)

	w.buf1.Reset()
	w.buf2.Reset()

	w.buf2.PutBE32int(len(symbols))

	w.symbols = make(map[string]uint32, len(symbols))

	for index, s := range symbols {
		w.symbols[s] = uint32(index)
		w.buf2.PutUvarintStr(s)
	}

	w.buf1.PutBE32int(w.buf2.Len())
	w.buf2.PutHash(w.crc32)

	err := w.write(w.buf1.Get(), w.buf2.Get())
	return errors.Wrap(err, "write symbols")
}

// ┌───────────────┬────────────────┬────────────────┐
// │ len <4b>      │ #names <4b>    │ #entries <4b>  │
// ├───────────────┴────────────────┴────────────────┤
// │ ┌─────────────────────────────────────────────┐ │
// │ │ ref(value_0) <4b>                           │ │
// │ ├─────────────────────────────────────────────┤ │
// │ │ ...                                         │ │
// │ ├─────────────────────────────────────────────┤ │
// │ │ ref(value_n) <4b>                           │ │
// │ └─────────────────────────────────────────────┘ │
// │                      . . .                      │
// ├─────────────────────────────────────────────────┤
// │ CRC32 <4b>                                      │
// └─────────────────────────────────────────────────┘
// Align to 4 bytes
// The values are sorted.
func (w *Writer) WriteLabelIndex(names []string, values []string) error {
	// NOTE(Alec), currently not need to do this check.
	// if len(values)%len(names) != 0 {
	// 	return errors.Errorf("invalid value list length %d for %d names", len(values), len(names))
	// }
	if err := w.ensureStage(idxStageLabelIndex); err != nil {
		return errors.Wrap(err, "ensure stage")
	}

	valt, err := NewStringTuples(values, len(names))
	if err != nil {
		return err
	}
	sort.Sort(valt)

	// Align beginning to 4 bytes for more efficient index list scans.
	if err := w.addPadding(4); err != nil {
		return err
	}

	w.labelIndexes = append(w.labelIndexes, hashEntry{
		keys:   names,
		offset: w.pos,
	})

	w.buf2.Reset()
	w.buf2.PutBE32int(len(names))
	w.buf2.PutBE32int(valt.Len())

	// here we have an index for the symbol file if v2, otherwise it's an offset
	for _, v := range valt.entries {
		index, ok := w.symbols[v]
		if !ok {
			return errors.Errorf("symbol entry for %q does not exist", v)
		}
		w.buf2.PutBE32(index)
	}

	w.buf1.Reset()
	w.buf1.PutBE32int(w.buf2.Len())

	w.buf2.PutHash(w.crc32)

	err = w.write(w.buf1.Get(), w.buf2.Get())
	return errors.Wrap(err, "write label index")
}

// writeOffsetTable writes a sequence of readable hash entries.
// ┌─────────────────────┬────────────────────┐
// │ len <4b>            │ #entries <4b>      │
// ├─────────────────────┴────────────────────┤
// │ ┌──────────────────────────────────────┐ │
// │ │  n = #strs <uvarint>                 │ │
// │ ├──────────────────────┬───────────────┤ │
// │ │ len(str_1) <uvarint> │ str_1 <bytes> │ │
// │ ├──────────────────────┴───────────────┤ │
// │ │  ...                                 │ │
// │ ├──────────────────────┬───────────────┤ │
// │ │ len(str_n) <uvarint> │ str_n <bytes> │ │
// │ ├──────────────────────┴───────────────┤ │
// │ │  offset <uvarint>                    │ │
// │ └──────────────────────────────────────┘ │
// │                  . . .                   │
// ├──────────────────────────────────────────┤
// │  CRC32 <4b>                              │
// └──────────────────────────────────────────┘
// Need to record the starting offset in the TOC first.
func (w *Writer) writeOffsetTable(entries []hashEntry) error {
	w.buf2.Reset()
	w.buf2.PutBE32int(len(entries))

	for _, e := range entries {
		w.buf2.PutUvarint(len(e.keys))
		for _, k := range e.keys {
			w.buf2.PutUvarintStr(k)
		}
		w.buf2.PutUvarint64(e.offset)
	}

	w.buf1.Reset()
	w.buf1.PutBE32int(w.buf2.Len())
	w.buf2.PutHash(w.crc32)

	return w.write(w.buf1.Get(), w.buf2.Get())
}

// ┌────────────────────┬────────────────────┐
// │ len <4b>           │ #entries <4b>      │
// ├────────────────────┴────────────────────┤
// │ ┌─────────────────────────────────────┐ │
// │ │ ref(group posting 1) <uvarint>      │ │
// │ ├─────────────────────────────────────┤ │
// │ │ ...                                 │ │
// │ ├─────────────────────────────────────┤ │
// │ │ ref(group posting n) <uvarint>      │ │
// │ └─────────────────────────────────────┘ │
// ├─────────────────────────────────────────┤
// │ CRC32 <4b>                              │
// └─────────────────────────────────────────┘
func (w *Writer) writeGroupPostingsTable() error {
	w.buf2.Reset()
	w.buf2.PutBE32int(len(w.groupPostings))

	for _, off := range w.groupPostings {
		w.buf2.PutUvarint64(off)
	}

	w.buf1.Reset()
	w.buf1.PutBE32int(w.buf2.Len())
	w.buf2.PutHash(w.crc32)

	return w.write(w.buf1.Get(), w.buf2.Get())
}

const indexTOCLen = 8*8 + 4

func (w *Writer) writeTOC() error {
	w.buf1.Reset()

	w.buf1.PutBE64(w.toc.Symbols)
	w.buf1.PutBE64(w.toc.Series)
	w.buf1.PutBE64(w.toc.LabelIndices)
	w.buf1.PutBE64(w.toc.LabelIndicesTable)
	w.buf1.PutBE64(w.toc.Postings)
	w.buf1.PutBE64(w.toc.PostingsTable)
	w.buf1.PutBE64(w.toc.GroupPostings)
	w.buf1.PutBE64(w.toc.GroupPostingsTable)

	w.buf1.PutHash(w.crc32)

	return w.write(w.buf1.Get())
}

// ┌────────────────────┬────────────────────┐
// │ len <4b>           │ #entries <4b>      │
// ├────────────────────┴────────────────────┤
// │ ┌─────────────────────────────────────┐ │
// │ │ ref(series_1) <4b>                  │ │
// │ ├─────────────────────────────────────┤ │
// │ │ ...                                 │ │
// │ ├─────────────────────────────────────┤ │
// │ │ ref(series_n) <4b>                  │ │
// │ └─────────────────────────────────────┘ │
// ├─────────────────────────────────────────┤
// │ CRC32 <4b>                              │
// └─────────────────────────────────────────┘
//
// Align to 4 bytes.
func (w *Writer) WritePostings(name, value string, it Postings) error {
	if err := w.ensureStage(idxStagePostings); err != nil {
		return errors.Wrap(err, "ensure stage")
	}

	// Align beginning to 4 bytes for more efficient postings list scans.
	if err := w.addPadding(4); err != nil {
		return err
	}

	w.postings = append(w.postings, hashEntry{
		keys:   []string{name, value},
		offset: w.pos,
	})

	// Order of the references in the postings list does not imply order
	// of the series references within the persisted block they are mapped to.
	// We have to sort the new references again.
	refs := w.uint32s[:0]

	for it.Next() {
		offset, ok := w.seriesOffsets[it.At()]
		if !ok {
			return errors.Errorf("%p series for reference %d not found", w, it.At())
		}
		if offset > (1<<32)-1 {
			return errors.Errorf("series offset %d exceeds 4 bytes", offset)
		}
		refs = append(refs, uint32(offset))
	}
	if err := it.Err(); err != nil {
		return err
	}
	sort.Sort(uint32slice(refs))

	w.buf2.Reset()
	w.buf2.PutBE32int(len(refs)) // #entries

	for _, r := range refs {
		w.buf2.PutBE32(r)
	}
	w.uint32s = refs

	w.buf1.Reset()
	w.buf1.PutBE32int(w.buf2.Len())

	w.buf2.PutHash(w.crc32)

	err := w.write(w.buf1.Get(), w.buf2.Get())
	return errors.Wrap(err, "write postings")
}

// ┌────────────────────┬────────────────────┐
// │ len <4b>           │ #entries <4b>      │
// ├────────────────────┴────────────────────┤
// │ ┌─────────────────────────────────────┐ │
// │ │ ref(series_1) <uvarint>             │ │
// │ ├─────────────────────────────────────┤ │
// │ │ ...                                 │ │
// │ ├─────────────────────────────────────┤ │
// │ │ ref(series_n) <uvarint>             │ │
// │ └─────────────────────────────────────┘ │
// ├─────────────────────────────────────────┤
// │ CRC32 <4b>                              │
// └─────────────────────────────────────────┘
// Align to 4 bytes.
//
// The values passed in here are the series refs of a whole group, not the real offsets of series entries.
func (w *Writer) WriteGroupPostings(it Postings) error {
	if err := w.ensureStage(idxStageGroupPostings); err != nil {
		return errors.Wrap(err, "ensure stage")
	}

	// Align beginning to 4 bytes for more efficient postings list scans.
	if err := w.addPadding(4); err != nil {
		return err
	}

	w.groupPostings = append(w.groupPostings, w.pos)

	refs := w.uint32s[:0]
	for it.Next() {
		offset, ok := w.seriesOffsets[it.At()]
		if !ok {
			return errors.Errorf("%p series for reference %d not found", w, it.At())
		}
		// The logical group ref is the idx in the group array.
		if err := w.updateLogicalGroupRef(uint64(len(w.groupPostings) - 1), offset << 4); err != nil {
			return err
		}
		refs = append(refs, uint32(offset))
	}
	if err := it.Err(); err != nil {
		return err
	}
	sort.Sort(uint32slice(refs))

	w.buf2.Reset()
	w.buf2.PutBE32int(len(refs)) // #entries

	for _, r := range refs {
		w.buf2.PutUvarint32(r)
	}
	w.uint32s = refs

	w.buf1.Reset()
	w.buf1.PutBE32int(w.buf2.Len())

	w.buf2.PutHash(w.crc32)

	err := w.write(w.buf1.Get(), w.buf2.Get())
	return errors.Wrap(err, "write group postings")
}

func (w *Writer) updateLogicalGroupRef(ref uint64, pos uint64) error {
	if err := w.fbuf.Flush(); err != nil {
		return err
	}
	// NOTE(Alec), Read in O_RDWR will forward the cursor.
	if _, err := w.f.ReadAt(w.buf1.C[:], int64(pos)); err != nil { // Read the uvarint length.
		return err
	}
	_, n := binary.Uvarint(w.buf1.C[:])
	if _, err := w.f.Seek(int64(pos)+int64(n), 0); err != nil { // Skip the len of series entry.
		return err
	}
	binary.BigEndian.PutUint64(w.buf1.C[:], ref)
	if _, err := w.f.Write(w.buf1.C[:8]); err != nil { // Write ref.
		return err
	}
	if _, err := w.f.Seek(0, 2); err != nil { // Move to the end of the file.
		return err
	}
	return nil
}

type uint32slice []uint32

func (s uint32slice) Len() int           { return len(s) }
func (s uint32slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s uint32slice) Less(i, j int) bool { return s[i] < s[j] }

type hashEntry struct {
	keys   []string
	offset uint64
}

func (w *Writer) Close() error {
	if err := w.ensureStage(idxStageDone); err != nil {
		return err
	}
	if err := w.fbuf.Flush(); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	return w.f.Close()
}

// StringTuples provides access to a sorted list of string tuples.
type StringTuples interface {
	// Total number of tuples in the list.
	Len() int
	// At returns the tuple at position i.
	At(i int) ([]string, error)
}

type Reader struct {
	b ByteSlice

	// Close that releases the underlying resources of the byte slice.
	c io.Closer

	// Cached hashmaps of section offsets.
	labels map[string]uint64
	// LabelName to LabelValue to offset map.
	postings      map[string]map[string]uint64
	groupPostings []uint64
	// Cache of read symbols. Strings that are returned when reading from the
	// block are always backed by true strings held in here rather than
	// strings that are backed by byte slices from the mmap'd index file. This
	// prevents memory faults when applications work with read symbols after
	// the block has been unmapped. The older format has sparse indexes so a map
	// must be used, but the new format is not so we can use a slice.

	// Disable V1 here.
	// symbolsV1        map[uint32]string
	symbolsV2        []string
	symbolsTableSize uint64

	dec *Decoder

	version int
}

// ByteSlice abstracts a byte slice.
type ByteSlice interface {
	Len() int
	Range(start, end int) []byte
}

type realByteSlice []byte

func (b realByteSlice) Len() int {
	return len(b)
}

func (b realByteSlice) Range(start, end int) []byte {
	return b[start:end]
}

func (b realByteSlice) Sub(start, end int) ByteSlice {
	return b[start:end]
}

// NewReader returns a new index reader on the given byte slice. It automatically
// handles different format versions.
func NewReader(b ByteSlice) (*Reader, error) {
	return newReader(b, ioutil.NopCloser(nil))
}

// NewFileReader returns a new index reader against the given index file.
func NewFileReader(path string) (*Reader, error) {
	f, err := fileutil.OpenMmapFile(path)
	if err != nil {
		return nil, err
	}
	r, err := newReader(realByteSlice(f.Bytes()), f)
	if err != nil {
		var merr tsdb_errors.MultiError
		merr.Add(err)
		merr.Add(f.Close())
		return nil, merr
	}

	return r, nil
}

func newReader(b ByteSlice, c io.Closer) (*Reader, error) {
	r := &Reader{
		b:        b,
		c:        c,
		labels:   map[string]uint64{},
		postings: map[string]map[string]uint64{},
	}

	// Verify header.
	if r.b.Len() < HeaderLen {
		return nil, errors.Wrap(encoding.ErrInvalidSize, "index header")
	}
	if m := binary.BigEndian.Uint32(r.b.Range(0, 4)); m != MagicIndex {
		return nil, errors.Errorf("invalid magic number %x", m)
	}
	r.version = int(r.b.Range(4, 5)[0])

	if r.version != FormatGroup {
		return nil, errors.Errorf("unknown index file version %d", r.version)
	}

	toc, err := NewTOCFromByteSlice(b)
	if err != nil {
		return nil, errors.Wrap(err, "read TOC")
	}

	r.symbolsV2, err = ReadSymbolsV2(r.b, int(toc.Symbols))
	if err != nil {
		return nil, errors.Wrap(err, "read symbols")
	}

	// Use the strings already allocated by symbols, rather than
	// re-allocating them again below.
	// Additionally, calculate symbolsTableSize.
	allocatedSymbols := make(map[string]string, len(r.symbolsV2))
	for _, s := range r.symbolsV2 {
		r.symbolsTableSize += uint64(len(s) + 8)
		allocatedSymbols[s] = s
	}

	if err := ReadOffsetTable(r.b, toc.LabelIndicesTable, func(key []string, off uint64) error {
		if len(key) != 1 {
			return errors.Errorf("unexpected key length for label indices table %d", len(key))
		}

		r.labels[allocatedSymbols[key[0]]] = off
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "read label index table")
	}

	r.postings[""] = map[string]uint64{} // NOTE(Alec), this is to save one if branch.
	if err := ReadOffsetTable(r.b, toc.PostingsTable, func(key []string, off uint64) error {
		if len(key) != 2 {
			return errors.Errorf("unexpected key length for posting table %d", len(key))
		}
		if _, ok := r.postings[key[0]]; !ok {
			r.postings[allocatedSymbols[key[0]]] = map[string]uint64{}
		}
		r.postings[key[0]][allocatedSymbols[key[1]]] = off
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "read postings table")
	}

	if err := r.ReadGroupPostingsTable(toc.GroupPostingsTable); err != nil {
		return nil, errors.Wrap(err, "read group postings table")
	}

	r.dec = &Decoder{LookupSymbol: r.lookupSymbol}

	return r, nil
}

// Version returns the file format version of the underlying index.
func (r *Reader) Version() int {
	return r.version
}

// Range marks a byte range.
type Range struct {
	Start, End int64
}

// PostingsRanges returns a new map of byte range in the underlying index file
// for all postings lists.
func (r *Reader) PostingsRanges() (map[labels.Label]Range, error) {
	m := map[labels.Label]Range{}

	for k, e := range r.postings {
		for v, start := range e {
			d := encoding.NewDecbufAt(r.b, int(start), castagnoliTable)
			if d.Err() != nil {
				return nil, d.Err()
			}
			m[labels.Label{Name: k, Value: v}] = Range{
				Start: int64(start) + 4,
				End:   int64(start) + 4 + int64(d.Len()),
			}
		}
	}
	return m, nil
}

// ReadSymbols reads the symbol table fully into memory and allocates proper strings for them.
// Strings backed by the mmap'd memory would cause memory faults if applications keep using them
// after the reader is closed.
func ReadSymbols(bs ByteSlice, version int, off int) ([]string, map[uint32]string, error) {
	if off == 0 {
		return nil, nil, nil
	}
	d := encoding.NewDecbufAt(bs, off, castagnoliTable)

	var (
		origLen     = d.Len()
		cnt         = d.Be32int()
		basePos     = uint32(off) + 4
		nextPos     = basePos + uint32(origLen-d.Len())
		symbolSlice []string
		symbols     = map[uint32]string{}
	)
	if version == FormatV2 {
		symbolSlice = make([]string, 0, cnt)
	}

	for d.Err() == nil && d.Len() > 0 && cnt > 0 {
		s := d.UvarintStr()

		if version == FormatV2 {
			symbolSlice = append(symbolSlice, s)
		} else {
			symbols[nextPos] = s
			nextPos = basePos + uint32(origLen-d.Len())
		}
		cnt--
	}
	return symbolSlice, symbols, errors.Wrap(d.Err(), "read symbols")
}

// NOTE(Alec), currently we disable V1.
func ReadSymbolsV2(bs ByteSlice, off int) ([]string, error) {
	if off == 0 {
		return nil, nil
	}
	d := encoding.NewDecbufAt(bs, off, castagnoliTable)

	cnt := d.Be32int()
	symbolSlice := make([]string, 0, cnt)

	for d.Err() == nil && d.Len() > 0 && cnt > 0 {
		s := d.UvarintStr()
		symbolSlice = append(symbolSlice, s)
		cnt--
	}
	return symbolSlice, errors.Wrap(d.Err(), "read symbols v2")
}

// ReadOffsetTable reads an offset table and at the given position calls f for each
// found entry. If f returns an error it stops decoding and returns the received error.
func ReadOffsetTable(bs ByteSlice, off uint64, f func([]string, uint64) error) error {
	d := encoding.NewDecbufAt(bs, int(off), castagnoliTable)
	cnt := d.Be32()

	for d.Err() == nil && d.Len() > 0 && cnt > 0 {
		keyCount := d.Uvarint()
		keys := make([]string, 0, keyCount)

		for i := 0; i < keyCount; i++ {
			keys = append(keys, d.UvarintStr())
		}
		o := d.Uvarint64()
		if d.Err() != nil {
			break
		}
		if err := f(keys, o); err != nil {
			return err
		}
		cnt--
	}
	return d.Err()
}

// ┌────────────────────┬────────────────────┐
// │ len <4b>           │ #entries <4b>      │
// ├────────────────────┴────────────────────┤
// │ ┌─────────────────────────────────────┐ │
// │ │ ref(series_1) <uvarint>             │ │
// │ ├─────────────────────────────────────┤ │
// │ │ ...                                 │ │
// │ ├─────────────────────────────────────┤ │
// │ │ ref(series_n) <uvarint>             │ │
// │ └─────────────────────────────────────┘ │
// ├─────────────────────────────────────────┤
// │ CRC32 <4b>                              │
// └─────────────────────────────────────────┘
func (r *Reader) ReadGroupPostingsTable(off uint64) error {
	// NOTE(Alec), this step will read len, verify crc32, and return the data part.
	d := encoding.NewDecbufAt(r.b, int(off), castagnoliTable)
	cnt := d.Be32()

	for d.Err() == nil && d.Len() > 0 && cnt > 0 {
		ref := d.Uvarint64()
		r.groupPostings = append(r.groupPostings, ref)
		cnt--
	}
	return d.Err()
}

// Close the reader and its underlying resources.
func (r *Reader) Close() error {
	return r.c.Close()
}

func (r *Reader) lookupSymbol(o uint32) (string, error) {
	if int(o) < len(r.symbolsV2) {
		return r.symbolsV2[o], nil
	}
	return "", errors.Errorf("unknown symbol offset %d", o)
}

// Symbols returns a set of symbols that exist within the index.
func (r *Reader) Symbols() (map[string]struct{}, error) {
	res := make(map[string]struct{}, len(r.symbolsV2))

	for _, s := range r.symbolsV2 {
		res[s] = struct{}{}
	}
	return res, nil
}

// SymbolTableSize returns the symbol table size in bytes.
func (r *Reader) SymbolTableSize() uint64 {
	return r.symbolsTableSize
}

// LabelValues returns value tuples that exist for the given label name tuples.
func (r *Reader) LabelValues(names ...string) (StringTuples, error) {

	key := strings.Join(names, labelNameSeperator)
	off, ok := r.labels[key]
	if !ok {
		// XXX(fabxc): hot fix. Should return a partial data error and handle cases
		// where the entire block has no data gracefully.
		return emptyStringTuples{}, nil
		//return nil, fmt.Errorf("label index doesn't exist")
	}

	d := encoding.NewDecbufAt(r.b, int(off), castagnoliTable)

	nc := d.Be32int()
	d.Be32() // consume unused value entry count.

	if d.Err() != nil {
		return nil, errors.Wrap(d.Err(), "read label value index")
	}
	st := &serializedStringTuples{
		idsCount: nc,
		idsBytes: d.Get(),
		lookup:   r.lookupSymbol,
	}
	return st, nil
}

type emptyStringTuples struct{}

func (emptyStringTuples) At(i int) ([]string, error) { return nil, nil }
func (emptyStringTuples) Len() int                   { return 0 }

// LabelIndices returns a slice of label names for which labels or label tuples value indices exist.
// NOTE: This is deprecated. Use `LabelNames()` instead.
func (r *Reader) LabelIndices() ([][]string, error) {
	var res [][]string
	for s := range r.labels {
		res = append(res, strings.Split(s, labelNameSeperator))
	}
	return res, nil
}

// Series reads the series with the given ID and writes its labels and chunks into lbls and chks.
func (r *Reader) Series(id uint64, lbls *labels.Labels, chks *[]chunkenc.Meta) error {
	offset := int(id << 4)
	// In version 2 series IDs are no longer exact references but series are 16-byte padded
	// and the ID is the multiple of 16 of the actual position.

	// d := encoding.NewDecbufUvarintAt(r.b, int(offset), castagnoliTable)
	// if d.Err() != nil {
	// 	return d.Err()
	// }

	if r.b.Len() < offset+binary.MaxVarintLen32 {
		return encoding.ErrInvalidSize
	}

	b := r.b.Range(offset, offset+binary.MaxVarintLen32)
	l, n := binary.Uvarint(b)
	if n <= 0 || n > binary.MaxVarintLen32 {
		return errors.Errorf("invalid uvarint %d", n)
	}

	if r.b.Len() < offset+n+8+int(l)+4 {
		return encoding.ErrInvalidSize
	}

	// Load bytes holding the contents plus a CRC32 checksum.
	b = r.b.Range(offset+n, offset+n+8+int(l)+4)
	dec := encoding.Decbuf{B: b[8:len(b)-4]}
	
	if dec.Crc32(castagnoliTable) != binary.BigEndian.Uint32(b[len(b)-4:]) {
		return encoding.ErrInvalidChecksum
	}

	return errors.Wrap(r.dec.Series(dec.Get(), binary.BigEndian.Uint64(b[:8]), lbls, chks), "read series")
}

// Postings returns a postings list for the given label pair.
func (r *Reader) Postings(name, value string) (Postings, error) {
	e, ok := r.postings[name]
	if !ok {
		return EmptyPostings(), nil
	}
	off, ok := e[value]
	if !ok {
		return EmptyPostings(), nil
	}
	d := encoding.NewDecbufAt(r.b, int(off), castagnoliTable)
	if d.Err() != nil {
		return nil, errors.Wrap(d.Err(), "get postings entry")
	}
	_, p, err := r.dec.Postings(d.Get())
	if err != nil {
		return nil, errors.Wrap(err, "decode postings")
	}
	return p, nil
}

// ┌────────────────────┬────────────────────┐
// │ len <4b>           │ #entries <4b>      │
// ├────────────────────┴────────────────────┤
// │ ┌─────────────────────────────────────┐ │
// │ │ ref(series_1) <uvarint>             │ │
// │ ├─────────────────────────────────────┤ │
// │ │ ...                                 │ │
// │ ├─────────────────────────────────────┤ │
// │ │ ref(series_n) <uvarint>             │ │
// │ └─────────────────────────────────────┘ │
// ├─────────────────────────────────────────┤
// │ CRC32 <4b>                              │
// └─────────────────────────────────────────┘
// Return a Postings list containing the (offset / 16) of each series
//
// Here the group_ref is the index of the group_postings_table.
func (r *Reader) GroupPostings(ref uint64) (Postings, error) {
	if ref == AllGroupPostings {
		return NewIncrementPostings(0, len(r.groupPostings)), nil
	}
	if int(ref) >= len(r.groupPostings) {
		return nil, errors.Errorf("Fail to read group postings, group index:%d not existed", ref)
	}

	offset := r.groupPostings[ref]
	d := encoding.NewDecbufAt(r.b, int(offset), castagnoliTable)
	d.Be32()

	return newUvarintPostings(&d), d.Err()
}

// SortedPostings returns the given postings list reordered so that the backing series
// are sorted.
func (r *Reader) SortedPostings(p Postings) Postings {
	return p
}

func (r *Reader) SortedGroupPostings(p Postings) Postings {
	return p
}

// Size returns the size of an index file.
func (r *Reader) Size() int64 {
	return int64(r.b.Len())
}

// LabelNames returns all the unique label names present in the index.
func (r *Reader) LabelNames() ([]string, error) {
	labelNamesMap := make(map[string]struct{}, len(r.labels))
	for key := range r.labels {
		// 'key' contains the label names concatenated with the
		// delimiter 'labelNameSeperator'.
		names := strings.Split(key, labelNameSeperator)
		for _, name := range names {
			if name == allPostingsKey.Name {
				// This is not from any metric.
				// It is basically an empty label name.
				continue
			}
			labelNamesMap[name] = struct{}{}
		}
	}
	labelNames := make([]string, 0, len(labelNamesMap))
	for name := range labelNamesMap {
		labelNames = append(labelNames, name)
	}
	sort.Strings(labelNames)
	return labelNames, nil
}

type stringTuples struct {
	length  int      // tuple length
	entries []string // flattened tuple entries
}

func NewStringTuples(entries []string, length int) (*stringTuples, error) {
	if len(entries)%length != 0 {
		return nil, errors.Wrap(encoding.ErrInvalidSize, "string tuple list")
	}
	return &stringTuples{entries: entries, length: length}, nil
}

func (t *stringTuples) Len() int                   { return len(t.entries) / t.length }
func (t *stringTuples) At(i int) ([]string, error) { return t.entries[i : i+t.length], nil }

func (t *stringTuples) Swap(i, j int) {
	c := make([]string, t.length)
	copy(c, t.entries[i:i+t.length])

	for k := 0; k < t.length; k++ {
		t.entries[i+k] = t.entries[j+k]
		t.entries[j+k] = c[k]
	}
}

func (t *stringTuples) Less(i, j int) bool {
	for k := 0; k < t.length; k++ {
		d := strings.Compare(t.entries[i+k], t.entries[j+k])

		if d < 0 {
			return true
		}
		if d > 0 {
			return false
		}
	}
	return false
}

type serializedStringTuples struct {
	idsCount int
	idsBytes []byte // bytes containing the ids pointing to the string in the lookup table.
	lookup   func(uint32) (string, error)
}

func (t *serializedStringTuples) Len() int {
	return len(t.idsBytes) / (4 * t.idsCount)
}

func (t *serializedStringTuples) At(i int) ([]string, error) {
	if len(t.idsBytes) < (i+t.idsCount)*4 {
		return nil, encoding.ErrInvalidSize
	}
	res := make([]string, 0, t.idsCount)

	for k := 0; k < t.idsCount; k++ {
		offset := binary.BigEndian.Uint32(t.idsBytes[(i+k)*4:])

		s, err := t.lookup(offset)
		if err != nil {
			return nil, errors.Wrap(err, "symbol lookup")
		}
		res = append(res, s)
	}

	return res, nil
}

// Decoder provides decoding methods for the v1 and v2 index file format.
//
// It currently does not contain decoding methods for all entry types but can be extended
// by them if there's demand.
type Decoder struct {
	LookupSymbol func(uint32) (string, error)
}

// Postings returns a postings list for b and its number of elements.
func (dec *Decoder) Postings(b []byte) (int, Postings, error) {
	d := encoding.Decbuf{B: b}
	n := d.Be32int()
	l := d.Get()
	return n, newBigEndianPostings(l), d.Err()
}

// ┌────────────────────────────────────────────────────────────────┐
// │ len <uvarint>                                                  │
// ├────────────────────────────────────────────────────────────────┤
// │ group_ref(logical) <8b>                                        │
// ├────────────────────────────────────────────────────────────────┤
// │ ┌────────────────────────────────────────────────────────────┐ │
// │ │           labels count <uvarint64>                         │ │
// │ ├────────────────────────────────────────────────────────────┤ │
// │ │    ┌────────────────────────────────────────────┐          │ │
// │ │    │ ref(l_i.name) <uvarint32>                  │          │ │
// │ │    ├────────────────────────────────────────────┤          │ │
// │ │    │ ref(l_i.value) <uvarint32>                 │          │ │
// │ │    └────────────────────────────────────────────┘          │ │
// │ │                   ...                                      │ │
// │ ├────────────────────────────────────────────────────────────┤ │
// │ │           chunks count <uvarint>                           │ │
// │ ├────────────────────────────────────────────────────────────┤ │
// │ │    ┌────────────────────────────────────────────┐          │ │
// │ │    │ min_time <varint>                          │          │ │
// │ │    ├────────────────────────────────────────────┤          │ │
// │ │    │ max_time - min_time <uvarint>              │          │ │
// │ │    ├────────────────────────────────────────────┤          │ │
// │ │    │ Group start offset <uvarint>               │          │ │
// │ │    ├────────────────────────────────────────────┤          │ │
// │ │    │ Series relative offset <uvarint>           │          │ │
// │ │    └────────────────────────────────────────────┘          │ │
// │ │    ┌────────────────────────────────────────────┐          │ │
// │ │    │ c_0.maxt - c_0.mint <uvarint>              │          │ │
// │ │    ├────────────────────────────────────────────┤          │ │
// │ │    │ c_i.mint - c_i-1.maxt <uvarint>            │          │ │
// │ │    ├────────────────────────────────────────────┤          │ │
// │ │    │ ref(c_i.gr) - ref(c_i-1.gr) <varint>       │          │ │
// │ │    ├────────────────────────────────────────────┤          │ │
// │ │    │ ref(c_i.sr) - ref(c_i-1.sr) <varint>       │          │ │
// │ │    └────────────────────────────────────────────┘          │ │
// │ │                   ...                                      │ │
// │ └────────────────────────────────────────────────────────────┘ │
// ├────────────────────────────────────────────────────────────────┤
// │ CRC32 <4b>                                                     │
// └────────────────────────────────────────────────────────────────┘
// Reference is the offset of Series entry / 16.
// Series decodes a series entry from the given byte slice into lset and chks.
func (dec *Decoder) Series(b []byte, lgr uint64, lbls *labels.Labels, chks *[]chunkenc.Meta) error {
	*lbls = (*lbls)[:0]
	*chks = (*chks)[:0]

	d := encoding.Decbuf{B: b}

	k := d.Uvarint() // Read labels count.

	for i := 0; i < k; i++ {
		lno := uint32(d.Uvarint())
		lvo := uint32(d.Uvarint())

		if d.Err() != nil {
			return errors.Wrap(d.Err(), "read series label offsets")
		}

		ln, err := dec.LookupSymbol(lno)
		if err != nil {
			return errors.Wrap(err, "lookup label name")
		}
		lv, err := dec.LookupSymbol(lvo)
		if err != nil {
			return errors.Wrap(err, "lookup label value")
		}

		*lbls = append(*lbls, labels.Label{Name: ln, Value: lv})
	}

	// Read the chunks meta data.
	k = d.Uvarint() // Read chunks count.

	if k == 0 {
		return nil
	}

	t0 := d.Varint64()
	maxt := int64(d.Uvarint64()) + t0
	ref0 := int64(d.Uvarint64())
	seriesRef0 := int64(d.Uvarint64())

	*chks = append(*chks, chunkenc.Meta{
		Ref:             uint64(ref0),
		SeriesRef:       uint64(seriesRef0),
		LogicalGroupRef: lgr,
		MinTime:         t0,
		MaxTime:         maxt,
	})
	t0 = maxt

	for i := 1; i < k; i++ {
		mint := int64(d.Uvarint64()) + t0
		maxt := int64(d.Uvarint64()) + mint

		ref0 += d.Varint64()
		seriesRef0 += d.Varint64()
		t0 = maxt

		if d.Err() != nil {
			return errors.Wrapf(d.Err(), "read meta for chunk %d", i)
		}

		*chks = append(*chks, chunkenc.Meta{
			Ref:             uint64(ref0),
			SeriesRef:       uint64(seriesRef0),
			LogicalGroupRef: lgr,
			MinTime:         mint,
			MaxTime:         maxt,
		})
	}
	return d.Err()
}
