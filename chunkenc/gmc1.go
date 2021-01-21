package chunkenc

import (
	"encoding/binary"
	// "fmt"
	"math"
	"math/bits"
	"sort"

	"github.com/pkg/errors"
)

type GM1Group struct {
	b         bstream
	leading   uint8
	trailing  uint8
	first     float64
	last      float64
	num       int
	tupleSize int
	mlz       uint8
	mtz       uint8
	xors      []uint64
}

func newGM1Group(first float64, tupleSize int) *GM1Group {
	return &GM1Group{leading: 0xff, first: first, tupleSize: tupleSize, mlz: 64, mtz: 64}
}

func (g *GM1Group) Append(value float64) {
	var deltaValue uint64
	if g.num % g.tupleSize == 0 {
		g.leading = 0xff
		g.trailing = 0
		deltaValue = math.Float64bits(value) ^ math.Float64bits(g.first)
	} else {
		deltaValue = math.Float64bits(value) ^ math.Float64bits(g.last)
	}
	if deltaValue == 0 {
		if len(g.xors) > 0 {
			g.Clean()
		}
		g.b.writeBit(zero)
	} else {
		lz := uint8(bits.LeadingZeros64(deltaValue))
		tz := uint8(bits.TrailingZeros64(deltaValue))
		if lz >= 32 {
			lz = 31
		}

		if g.leading != 0xff && lz >= g.leading && tz >= g.trailing { // Inside the current range.
			if lz < g.mlz {
				g.mlz = lz
			}
			if tz < g.mtz {
				g.mtz = tz
			}
			g.xors = append(g.xors, deltaValue)
			g.last = value
			return
		} else {
			if len(g.xors) > 0 {
				g.Clean()
			}
			g.b.writeBit(one) // First control bit.
			g.leading = lz
			g.trailing = tz
			g.b.writeBit(one) // Second control bit.
			g.b.writeBits(uint64(lz), 5)

			sigbits := 64 - g.leading - g.trailing
			g.b.writeBits(uint64(sigbits), 6)
			g.b.writeBits(deltaValue >> g.trailing, int(sigbits))
		}
	}
	g.last = value
	g.num += 1
}

// Append the first element in the tuple.
// NOTE(Alec), discard the second control bit.
func (g *GM1Group) AppendFirst(value float64) {
	var deltaValue uint64
	g.leading = 0xff
	g.trailing = 0
	deltaValue = math.Float64bits(value) ^ math.Float64bits(g.first)

	if deltaValue == 0 {
		g.b.writeBit(zero)
	} else {
		g.b.writeBit(one) // First control bit.
		lz := uint8(bits.LeadingZeros64(deltaValue))
		tz := uint8(bits.TrailingZeros64(deltaValue))
		if lz >= 32 {
			lz = 31
		}

		g.leading = lz
		g.trailing = tz
		g.b.writeBits(uint64(lz), 5)

		sigbits := 64 - g.leading - g.trailing
		g.b.writeBits(uint64(sigbits), 6)
		g.b.writeBits(deltaValue >> g.trailing, int(sigbits))
	}
	g.last = value
	g.num += 1
}

func (g *GM1Group) Clean() {
	g.num += len(g.xors)
	if len(g.xors) * int(g.mlz + g.mtz - g.leading - g.trailing) > 11 {
		// Use new leading/trailing zero numbers.
		g.leading = g.mlz
		g.trailing = g.mtz

		// Write the first value in cache.
		g.b.writeBit(one) // First control bit.
		g.b.writeBit(one) // Second control bit.
		g.b.writeBits(uint64(g.leading), 5)

		sigbits := 64 - g.leading - g.trailing
		g.b.writeBits(uint64(sigbits), 6)
		g.b.writeBits(g.xors[0] >> g.trailing, int(sigbits))
		g.xors = g.xors[1:]
	}
	for _, xor := range g.xors {
		g.b.writeBit(one)  // First control bit.
		g.b.writeBit(zero) // Second control bit.
		g.b.writeBits(xor >> g.trailing, int(64 - g.leading - g.trailing))
	}
	g.xors = g.xors[:0]
	g.mlz = 64
	g.mtz = 64
}

func (g *GM1Group) Size() int {
	return g.b.Size()
}

func (g *GM1Group) WritePos() int {
	return g.b.WritePos()
}

func (g *GM1Group) ReadBitsAt(pos int, num int) (uint64, error) {
	return g.b.ReadBitsAt(pos, num)
}

func (g *GM1Group) ReadUvarintAt(pos int) (uint64, int, error) {
	return g.b.ReadUvarintAt(pos)
}

func (g *GM1Group) ReadVarintAt(pos int) (int64, int, error) {
	return g.b.ReadVarintAt(pos)
}

// ┌────────────────────┬────────────────────┬───────┬────────────────────┬────────────────────┐
// │ delta_t1  <varint> │ delta_t2  <varint> │ . . . │ delta_tn  <varint> │alignment <uvarint> │
// ├────────────────────┴────────────────────┴───────┴────────────────────┴────────────────────┤
// │ ┌───────────────────────────────────────────────────────────────────────────────────────┐ │
// │ │ pos_1 <alignment bits>                                                                │ │
// │ ├───────────────────────────────────────────────────────────────────────────────────────┤ │
// │ │                . . .                                                                  │ │
// │ ├───────────────────────────────────────────────────────────────────────────────────────┤ │
// │ │ pos_n <alignment bits>                                                                │ │
// │ └───────────────────────────────────────────────────────────────────────────────────────┘ │
// └───────────────────────────────────────────────────────────────────────────────────────────┘
// 
// The first group will not store positions.
type GM1Time struct {
	b bstream
	c [binary.MaxVarintLen64]byte
}

func (g *GM1Time) AppendDeltaT(delta int64) {
	encoded := binary.PutVarint(g.c[:], delta)
	for i := 0; i < encoded; i++ {
		g.b.writeByte(g.c[i])
	}
}

func (g *GM1Time) AppendOffBits(offsets []uint64, bits int) {
	// Put the alignment.
	encoded := binary.PutUvarint(g.c[:], uint64(bits))
	for i := 0; i < encoded; i++ {
		g.b.writeByte(g.c[i])
	}
	for _, pos := range offsets {
		g.b.writeBits(pos, bits)
	}
}

func (g *GM1Time) ReadBitsAt(pos int, num int) (uint64, error) {
	return g.b.ReadBitsAt(pos, num)
}

func (g *GM1Time) ReadUvarintAt(pos int) (uint64, int, error) {
	return g.b.ReadUvarintAt(pos)
}

func (g *GM1Time) ReadVarintAt(pos int) (int64, int, error) {
	return g.b.ReadVarintAt(pos)
}

var (
	ErrEmptyGMC = errors.Errorf("empty GMC")
	ErrGMCOutOfBounds = errors.Errorf("GMC out of bounds")
	ErrGMCSeriesNumNotMatch = errors.Errorf("GMC series number not matched")
)
type GroupMemoryChunk1 struct {
	groupNum  int
	tupleSize int
	group     []*GM1Group
	time      []*GM1Time
	count     int
	startTime int64
}

func NewGroupMemoryChunk1(groupNum int) *GroupMemoryChunk1 {
	return &GroupMemoryChunk1{groupNum: groupNum, tupleSize: DefaultTupleSize, startTime: math.MaxInt64}
}

// Encoding returns the encoding type.
func (c *GroupMemoryChunk1) Encoding() Encoding {
	return EncGMC1
}

// Only to implement the interface here.
func (c *GroupMemoryChunk1) Bytes() []byte {
	return nil
}

func (c *GroupMemoryChunk1) Appender() (Appender, error) {
	return &GM1Appender{chunk: c}, nil
}

func (c *GroupMemoryChunk1) gmcIterator() *GM1Iterator {
	return &GM1Iterator{chunk: c}
}

func (c *GroupMemoryChunk1) IteratorGroup(t int64, seriesNum int) Iterator {
	return newGM1Iterator(c, t, seriesNum)
}

// Iterator implements the Chunk interface.
func (c *GroupMemoryChunk1) Iterator() Iterator {
	return c.gmcIterator()
}

func (c *GroupMemoryChunk1) NumSamples() int {
	return c.count * c.groupNum
}

// Number of timestamps in [mint, maxt].
func (c *GroupMemoryChunk1) NumBetween(mint int64, maxt int64) int {
	it := c.gmcIterator()
	return it.numBetween(mint, maxt)
}

// Size of all bstream.
func (c *GroupMemoryChunk1) Size() int {
	var s int
	for _, t := range c.time {
		s += t.b.Size()
	}
	for _, g := range c.group {
		s += g.b.Size()
	}
	return s
}

func (c *GroupMemoryChunk1) MinTime() int64 { return c.startTime }
func (c *GroupMemoryChunk1) Count() int { return c.count }
func (c *GroupMemoryChunk1) Chunk() Chunk { return c }

func (c *GroupMemoryChunk1) Clean() {
	for _, g := range c.group {
		if len(g.xors) > 0 {
			g.Clean()
		}
	}
}

type GM1Appender struct {
	chunk *GroupMemoryChunk1
}

func (a *GM1Appender) Append(t int64, v float64) {}
func (a *GM1Appender) AppendGroup(t int64, vals []float64) {
	if len(a.chunk.group) == 0 {
		if a.chunk.groupNum == 0 {
			a.chunk.groupNum = len(vals)
		}
		a.chunk.startTime = t
		for _, val := range vals {
			g := newGM1Group(val, a.chunk.tupleSize)
			g.AppendFirst(val)
			a.chunk.group = append(a.chunk.group, g)
		}
		gt := &GM1Time{}
		gt.AppendDeltaT(0)
		a.chunk.time = append(a.chunk.time, gt)
		a.chunk.count += 1
	} else if len(vals) == a.chunk.groupNum {
		mod := a.chunk.count % a.chunk.tupleSize
		if mod == 0 {
			// Append time index slot.
			offsets := make([]uint64, a.chunk.groupNum)
			var pos uint64
			var l int
			var n int
			for i, grp := range a.chunk.group {
				pos = uint64(grp.WritePos())
				l = bits.Len64(pos)
				if l > n {
					n = l
				}
				offsets[i] = pos
			}

			// Add the starting offsets for next slot.
			a.chunk.time[len(a.chunk.time)-1].AppendOffBits(offsets, n)
			gt := &GM1Time{}
			gt.AppendDeltaT(t - a.chunk.startTime)
			a.chunk.time = append(a.chunk.time, gt)
			for i, val := range vals {
				a.chunk.group[i].AppendFirst(val)
			}
			a.chunk.count += 1
		} else {
			a.chunk.time[len(a.chunk.time)-1].AppendDeltaT(t - a.chunk.startTime)
			if mod == a.chunk.tupleSize - 1 {
				for i, val := range vals {
					a.chunk.group[i].Append(val)
					if len(a.chunk.group[i].xors) > 0 {
						a.chunk.group[i].Clean()
					}
				}
			} else {
				for i, val := range vals {
					a.chunk.group[i].Append(val)
				}
			}
			a.chunk.count += 1
		}
	}
}

type GM1Iterator struct {
	chunk         *GroupMemoryChunk1
	num           int // The cached num from chunk.
	seriesNum     int
	seriesOff     int
	timeNum       int
	timeNumInside int
	timeOff       int
	err           error
	leading       uint8
	trailing      uint8
	t             int64
	v             float64
	cachedXors    []uint64
}

// NOTE(Alec): it's user's responsibility to lock before calling (head.go).
func newGM1Iterator(chunk *GroupMemoryChunk1, timestamp int64, seriesNum int) *GM1Iterator {
	it := &GM1Iterator{chunk: chunk, t: math.MinInt64, seriesNum: seriesNum}
	if seriesNum >= it.chunk.groupNum {
		it.err = errors.Errorf("seriesNum >= it.chunk.groupNum")
		return it
	}
	it.num = it.chunk.group[it.seriesNum].num
	it.cachedXors = make([]uint64, len(chunk.group[seriesNum].xors))
	copy(it.cachedXors, chunk.group[seriesNum].xors)

	it.timeNum, it.timeNumInside, it.timeOff, it.err = it.findTime(timestamp)
	if it.err != nil {
		return it
	}

	it.v = it.chunk.group[it.seriesNum].first
	_, it.err = it.findSeries()

	return it
}

func (it *GM1Iterator) At() (int64, float64) {
	return it.t, it.v
}

func (it *GM1Iterator) Next() bool {
	sampleIdx := it.timeNum * it.chunk.tupleSize + it.timeNumInside
	if it.err != nil || sampleIdx >= it.num + len(it.cachedXors) {
		return false
	}

	var delta int64
	var val   float64
	var n     int

	if it.timeNumInside % it.chunk.tupleSize == 0 {
		if it.timeNumInside > 0 {
			// Goto next tuple.
			it.timeNum += 1
			it.timeNumInside = 0
			it.timeOff = 0
			it.v = it.chunk.group[it.seriesNum].first // Reset the XOR base.
		}
		// Decode timestamp.
		delta, n, it.err = it.chunk.time[it.timeNum].ReadVarintAt(it.timeOff)
		if it.err != nil {
			return false
		}
		it.t = it.chunk.startTime + delta
		it.timeOff += n << 3

		// Decode value.
		val, n, it.err = it.decodeFirst(it.seriesOff, it.v)
	} else {
		// Decode timestamp.
		delta, n, it.err = it.chunk.time[it.timeNum].ReadVarintAt(it.timeOff)
		if it.err != nil {
			return false
		}
		it.t = it.chunk.startTime + delta
		it.timeOff += n << 3

		// Decode value.
		if sampleIdx < it.num {
			val, n, it.err = it.decode(it.seriesOff, it.v)
		} else {
			it.v = math.Float64frombits(math.Float64bits(it.v) ^ it.cachedXors[sampleIdx - it.num])
			it.timeNumInside++
			return true
		}
	}
	if it.err != nil {
		return false
	}

	it.v = val
	it.seriesOff += n
	it.timeNumInside++
	return true
}

func (it *GM1Iterator) Seek(t int64) bool {
	if it.t >= t {
		return true
	}
	if it.err != nil {
		return false
	}

	it.leading = 0
	it.trailing = 0
	it.timeNum, it.timeNumInside, it.timeOff, it.err = it.findTime(t)
	if it.err != nil {
		return false
	}
	it.v = it.chunk.group[it.seriesNum].first
	it.seriesOff = 0
	_, it.err = it.findSeries()
	if it.err != nil {
		return false
	}
	return it.Next()
}

func (it *GM1Iterator) Err() error {
	return it.err
}

// <tuple num, idx inside tuple, offset>, find the first one >= time.
func (it *GM1Iterator) findTime(t int64) (int, int, int, error) {
	if it.chunk.count == 0 {
		return -1, -1, -1, ErrEmptyGMC
	}
	if t <= it.chunk.startTime {
		return 0, 0, 0, nil
	}
	num := len(it.chunk.time)
	t -= it.chunk.startTime // Subtract the base.
	first := sort.Search(num, func(i int) bool { // Binary search among tuples.
		temp, _, _ := it.chunk.time[i].ReadVarintAt(0)
		return temp > t
	})
	if first != 0 {
		first -= 1
	}

	// NOTE(Alec), normally stop at the current tuple or the next tuple.
	for first < num {
		pos := 0
		end := it.chunk.tupleSize
		if first == num - 1 && it.chunk.count % it.chunk.tupleSize != 0 {
			end = it.chunk.count % it.chunk.tupleSize
		}
		for i := 0; i < end; i++ {
			temp, n, err := it.chunk.time[first].ReadVarintAt(pos)
			if err != nil {
				return -1, -1, -1, err
			}
			if temp >= t {
				if first * it.chunk.tupleSize + i >= it.num + len(it.cachedXors) {
					return -1, -1, -1, ErrGMCOutOfBounds
				}
				return first, i, pos, nil
			}
			pos += n << 3
		}
		first += 1
	}
	return -1, -1, -1, ErrGMCOutOfBounds
}

// Return seriesOff.
func (it *GM1Iterator) findSeries() (int, error) {
	if it.timeNum == 0 {
		if it.timeNumInside == 0 {
			return 0, nil
		} else {
			if it.timeNumInside > 0 {
				val, n, err := it.decodeFirst(it.seriesOff, it.v)
				if err != nil {
					return 0, err
				}
				it.v = val
				it.seriesOff += n
			}
			for i := 1; i < it.timeNumInside; i++ {
				val, n, err := it.decode(it.seriesOff, it.v)
				if err != nil {
					return 0, err
				}
				it.v = val
				it.seriesOff += n
			}
			return it.seriesOff, nil
		}
	}
	var pos   int
	var align uint64
	var bits  uint64
	var n     int
	for i := 0; i < it.chunk.tupleSize; i++ { // Read tupleSize deltas.
		_, n, it.err = it.chunk.time[it.timeNum-1].ReadVarintAt(pos)
		if it.err != nil {
			return 0, it.err
		}
		pos += n << 3
	}
	// Read alignment.
	align, n, it.err = it.chunk.time[it.timeNum-1].ReadUvarintAt(pos)
	if it.err != nil {
		return 0, it.err
	}
	pos += n << 3

	bits, it.err = it.chunk.time[it.timeNum-1].ReadBitsAt(pos + int(align) * it.seriesNum, int(align))
	if it.err != nil {
		return 0, it.err
	}
	it.seriesOff = int(bits)

	if it.timeNumInside != 0 {
		var val float64
		val, n, it.err = it.decodeFirst(it.seriesOff, it.v)
		if it.err != nil {
			return 0, it.err
		}
		it.v = val
		it.seriesOff += n
		for i := 1; i < it.timeNumInside; i++ {
			if it.timeNum * it.chunk.tupleSize + i >= it.num {
				it.v = math.Float64frombits(math.Float64bits(it.v) ^ it.cachedXors[it.timeNum * it.chunk.tupleSize + i - it.num])
			} else {
				val, n, it.err = it.decode(it.seriesOff, it.v)
				if it.err != nil {
					return 0, it.err
				}
				it.v = val
				it.seriesOff += n
			}
		}
	}
	return it.seriesOff, nil
}

// Return decoded value and decoded bits.
func (it *GM1Iterator) decode(offset int, last float64) (float64, int, error) {
	controlBit, err := it.chunk.group[it.seriesNum].b.ReadBitAt(offset)  // First control bit
	if err != nil {
		return 0.0, 0, err
	}

	if controlBit == zero {
		return last, 1, nil
	} else {
		controlBit, err = it.chunk.group[it.seriesNum].b.ReadBitAt(offset + 1) // Second control bit
		if err != nil {
			return 0.0, 0, err
		}
		var c int
		var bits uint64
		if controlBit != zero {
			bits, err = it.chunk.group[it.seriesNum].b.ReadBitsAt(offset + 2, 5)
			if err != nil {
				return 0.0, 0, err
			}
			it.leading = uint8(bits)

			bits, err = it.chunk.group[it.seriesNum].b.ReadBitsAt(offset + 7, 6)
			if err != nil {
				return 0.0, 0, err
			}

			// 0 significant bits here means we overflowed and we actually need 64; see comment in encoder
			if bits == 0 {
				bits = 64
			}
			it.trailing = uint8(64) - it.leading - uint8(bits)
			c += 11
		}

		bits, err = it.chunk.group[it.seriesNum].b.ReadBitsAt(offset + c + 2, 64 - int(it.leading) - int(it.trailing))
		if err != nil {
			return 0.0, 0, err
		}
		c += 64 - int(it.leading) - int(it.trailing)
		vbits := math.Float64bits(last)
		vbits ^= (bits << it.trailing)
		last = math.Float64frombits(vbits)

		return last, c + 2, nil
	}
}

// Decode the first value inside the tuple.
func (it *GM1Iterator) decodeFirst(offset int, last float64) (float64, int, error) {
	controlBit, err := it.chunk.group[it.seriesNum].b.ReadBitAt(offset)  // First control bit
	if err != nil {
		return 0.0, 0, err
	}

	if controlBit == zero {
		return last, 1, nil
	} else {
		var bits uint64
		bits, err = it.chunk.group[it.seriesNum].b.ReadBitsAt(offset + 1, 5)
		if err != nil {
			return 0.0, 0, err
		}
		it.leading = uint8(bits)

		bits, err = it.chunk.group[it.seriesNum].b.ReadBitsAt(offset + 6, 6)
		if err != nil {
			return 0.0, 0, err
		}

		// 0 significant bits here means we overflowed and we actually need 64; see comment in encoder
		if bits == 0 {
			bits = 64
		}
		it.trailing = uint8(64) - it.leading - uint8(bits)

		bits, err = it.chunk.group[it.seriesNum].b.ReadBitsAt(offset + 12, 64 - int(it.leading) - int(it.trailing))
		if err != nil {
			return 0.0, 0, err
		}
		vbits := math.Float64bits(last)
		vbits ^= (bits << it.trailing)
		last = math.Float64frombits(vbits)

		return last, 76 - int(it.leading) - int(it.trailing), nil
	}
}

// [mint, maxt].
func (it *GM1Iterator) numBetween(mint int64, maxt int64) int {
	if mint == math.MinInt64 {
		return it.chunk.count
	}

	timeNum1, timeNumInside1, _, err := it.findTime(mint)
	if err != nil || timeNum1 * it.chunk.tupleSize + timeNumInside1 >= it.chunk.count {
		return 0
	}
	timeNum2, timeNumInside2, timeOff2, err := it.findTime(mint)
	if err != nil {
		return 0
	}
	if timeNumInside2 % it.chunk.tupleSize == 0 && timeNumInside2 > 0 {
		timeNum2 += 1
		timeNumInside2 = 0
		timeOff2 = 0
	}
	delta, _, err := it.chunk.time[timeNum2].ReadVarintAt(timeOff2)
	if err != nil {
		return 0
	}
	if it.chunk.startTime + delta == maxt {
		return (timeNum2 - timeNum1) * it.chunk.tupleSize + timeNumInside2 - timeNumInside1 + 1
	} else {
		return (timeNum2 - timeNum1) * it.chunk.tupleSize + timeNumInside2 - timeNumInside1
	}
}

func (it *GM1Iterator) getValues(series int, d *[]float64, mint int64, maxt int64) {
	if series < it.chunk.groupNum {
		it.num = it.chunk.group[series].num
		it.seriesNum = series
		if mint == math.MinInt64 {
			tempLast := it.chunk.group[it.seriesNum].first
			var offset int
			var v      float64
			var n      int
			var err    error
			it.chunk.Clean()
			for i := 0; i < it.chunk.count; i++ {
				if i % it.chunk.tupleSize == 0 {
					tempLast = it.chunk.group[it.seriesNum].first
					v, n, err = it.decodeFirst(offset, tempLast)
				} else {
					v, n, err = it.decode(offset, tempLast)
					// fmt.Println("i", i, "offset", offset, "n", n)
				}
				if err != nil {
					return
				}
				tempLast = v
				*d = append(*d, tempLast)
				offset += n
			}
		} else {
			it.timeNum, it.timeNumInside, it.timeOff, it.err = it.findTime(mint)
			if it.err != nil {
				return
			}
			it.v = it.chunk.group[it.seriesNum].first
			it.seriesOff = 0
			_, it.err = it.findSeries()
			if it.err != nil {
				return
			}
			for it.Next() {
				if it.t > maxt {
					break
				}
				*d = append(*d, it.v)
			}
		}
	}
}