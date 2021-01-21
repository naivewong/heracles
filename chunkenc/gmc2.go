package chunkenc

import (
	"encoding/binary"
	// "fmt"
	"math"
	"math/bits"
	"sort"

	"github.com/pkg/errors"
)

// Todo(Alec), change with DefaultOptions.BlocksRanges.
func nextSplitPoint(t int64) int64 {
	return (t/3600000)*3600000 + 3600000
}

type GM2Group struct {
	b         bstream
	leading   uint8
	trailing  uint8
	bases     []float64
	last      float64
	num       int
	tupleSize int
	seriesOff int // Used when truncating the gmc2.
}

func newGM2Group(first float64, tupleSize int) *GM2Group {
	return &GM2Group{leading: 0xff, bases: []float64{first}, tupleSize: tupleSize}
}

func (g *GM2Group) Append(value float64) {
	var deltaValue uint64
	if g.num % g.tupleSize == 0 {
		g.leading = 0xff
		g.trailing = 0
		deltaValue = math.Float64bits(value) ^ math.Float64bits(g.bases[len(g.bases)-1])
	} else {
		deltaValue = math.Float64bits(value) ^ math.Float64bits(g.last)
	}
	if deltaValue == 0 {
		g.b.writeBit(zero)
	} else {
		g.b.writeBit(one) // First control bit.
		lz := uint8(bits.LeadingZeros64(deltaValue))
		tz := uint8(bits.TrailingZeros64(deltaValue))
		if lz >= 32 {
			lz = 31
		}
		if g.leading != 0xff && lz >= g.leading && tz >= g.trailing { // Inside the current range.
			g.b.writeBit(zero) // Second control bit.
			g.b.writeBits(deltaValue >> g.trailing, int(64 - g.leading - g.trailing))
		} else {
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

func (g *GM2Group) SetBase(base float64) {
	g.bases = append(g.bases, base)
	g.num = 0
}

func (g *GM2Group) PadToNextByte() {
	g.b.PadToNextByte()
}

func (g *GM2Group) Size() int {
	return g.b.Size()
}

func (g *GM2Group) WritePos() int {
	return g.b.WritePos()
}

func (g *GM2Group) ReadBitsAt(pos int, num int) (uint64, error) {
	return g.b.ReadBitsAt(pos, num)
}

func (g *GM2Group) ReadUvarintAt(pos int) (uint64, int, error) {
	return g.b.ReadUvarintAt(pos)
}

func (g *GM2Group) ReadVarintAt(pos int) (int64, int, error) {
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
type GM2Time struct {
	b bstream
	c [binary.MaxVarintLen64]byte
}

func (g *GM2Time) AppendDeltaT(delta int64) {
	encoded := binary.PutVarint(g.c[:], delta)
	for i := 0; i < encoded; i++ {
		g.b.writeByte(g.c[i])
	}
}

func (g *GM2Time) AppendOffBits(offsets []uint64, bits int) {
	// Put the alignment.
	encoded := binary.PutUvarint(g.c[:], uint64(bits))
	for i := 0; i < encoded; i++ {
		g.b.writeByte(g.c[i])
	}
	for _, pos := range offsets {
		g.b.writeBits(pos, bits)
	}
}

func (g *GM2Time) ReadBitsAt(pos int, num int) (uint64, error) {
	return g.b.ReadBitsAt(pos, num)
}

func (g *GM2Time) ReadUvarintAt(pos int) (uint64, int, error) {
	return g.b.ReadUvarintAt(pos)
}

func (g *GM2Time) ReadVarintAt(pos int) (int64, int, error) {
	return g.b.ReadVarintAt(pos)
}

type GroupMemoryChunk2 struct {
	groupNum   int
	tupleSize  int
	group      []*GM2Group
	time       []*GM2Time
	count      int
	startTimes []int64
	idx        []int
	tIdx       []int
	nextStop   int64
}

func NewGroupMemoryChunk2(groupNum int) *GroupMemoryChunk2 {
	return &GroupMemoryChunk2{groupNum: groupNum, tupleSize: 8}
}

// Encoding returns the encoding type.
func (c *GroupMemoryChunk2) Encoding() Encoding {
	return EncGMC1
}

// Only to implement the interface here.
func (c *GroupMemoryChunk2) Bytes() []byte {
	return nil
}

func (c *GroupMemoryChunk2) Appender() (Appender, error) {
	return &GM2Appender{chunk: c, seriesBases: make([]int, c.groupNum)}, nil
}

func (c *GroupMemoryChunk2) gmcIterator() *GM2Iterator {
	return &GM2Iterator{chunk: c}
}

func (c *GroupMemoryChunk2) IteratorGroup(t int64, seriesNum int) Iterator {
	return newGM2Iterator(c, t, seriesNum)
}

// Iterator implements the Chunk interface.
func (c *GroupMemoryChunk2) Iterator() Iterator {
	return c.gmcIterator()
}

func (c *GroupMemoryChunk2) NumSamples() int {
	return c.count * c.groupNum
}

// Number of timestamps in [mint, maxt].
func (c *GroupMemoryChunk2) NumBetween(mint int64, maxt int64) int {
	it := c.gmcIterator()
	return it.numBetween(mint, maxt)
}

// Size of all bstream.
func (c *GroupMemoryChunk2) Size() int {
	var s int
	for _, t := range c.time {
		s += t.b.Size()
	}
	for _, g := range c.group {
		s += g.b.Size()
	}
	return s
}

func (c *GroupMemoryChunk2) MinTime() int64 {
	if len(c.startTimes) > 0 {
		return c.startTimes[0]
	} else {
		return math.MaxInt64
	}
}
func (c *GroupMemoryChunk2) Count() int { return c.count }
func (c *GroupMemoryChunk2) Chunk() Chunk { return c }

func (c *GroupMemoryChunk2) TruncateBefore(mint int64) (*GroupMemoryChunk2, int) {
	if mint <= c.startTimes[0] {
		return nil, 0
	}
	gmc := &GroupMemoryChunk2{groupNum: c.groupNum, tupleSize: c.tupleSize}
	its := make([]*GM2Iterator, c.groupNum)
	for i := range its {
		its[i] = newGM2Iterator(c, mint, i)
	}
	if its[0].bIdx == len(its[0].chunk.startTimes) - 1 && its[0].currentSample == its[0].numSamples {
		return gmc, c.count
	}
	oldTimeNum := its[0].timeNum
	if its[0].currentSample == 0 {
		gmc.count = c.count - c.idx[its[0].bIdx]
		gmc.time = c.time[c.tIdx[its[0].bIdx]:]
		gmc.startTimes = c.startTimes[its[0].bIdx:]
		gmc.nextStop = c.nextStop
		for i := range c.idx[its[0].bIdx:] {
			gmc.idx = append(gmc.idx, c.idx[its[0].bIdx+i] - (c.count - gmc.count))
			gmc.tIdx = append(gmc.tIdx, c.tIdx[its[0].bIdx+i] - oldTimeNum)
		}
		for i, it := range its {
			gmc.group = append(gmc.group, &GM2Group{
				leading:   c.group[i].leading,
				trailing:  c.group[i].trailing,
				bases:     c.group[i].bases[it.bIdx:],
				last:      c.group[i].last,
				num:       c.group[i].num - c.idx[it.bIdx],
				seriesOff: c.group[i].seriesOff,
			})
			gmc.group[i].b.count = c.group[i].b.count

			// NOTE(Alec), don't forget to pad it.
			pad := it.seriesOff % 8
			if pad != 0 {
				it.seriesOff += 8 - pad
			}
			
			gmc.group[i].b.stream = append(gmc.group[i].b.stream, c.group[i].b.stream[it.seriesOff>>3:]...)
			gmc.group[i].seriesOff += it.seriesOff
		}
		return gmc, c.idx[its[0].bIdx]
	}
	tempTs := []int64{}
	tempVals := [][]float64{}
	for its[0].Next() {
		t, v := its[0].At()
		tempVals1 := make([]float64, gmc.groupNum)
		tempVals1[0] = v
		for i, it := range its[1:] {
			it.Next()
			_, v = it.At()
			tempVals1[i+1] = v
		}
		tempTs = append(tempTs, t)
		tempVals = append(tempVals, tempVals1)
		if its[0].currentSample == its[0].numSamples {
			// Break when reaching the next block.
			break
		}
	}
	seriesBases := make([]int, gmc.groupNum)
	for i, it := range its {
		// NOTE(Alec), seriesOff needs to be updated before append the new data.
		if !(its[0].bIdx == len(its[0].chunk.startTimes) - 1 && its[0].currentSample == its[0].numSamples) {
			// Pad the seriesOff to next byte.
			pad := it.seriesOff % 8
			if pad != 0 {
				it.seriesOff += 8 - pad
			}
		}
		seriesBases[i] = c.group[i].seriesOff + it.seriesOff
	}
	app := &GM2Appender{chunk: gmc, seriesBases: seriesBases}
	for i := range tempTs {
		app.AppendGroup(tempTs[i], tempVals[i])
	}

	if !(its[0].bIdx == len(its[0].chunk.startTimes) - 1 && its[0].currentSample == its[0].numSamples) {
		// If it's in the last block, you don't need to add the offsets of the series slot.
		gmc.count += c.count - c.idx[its[0].bIdx+1]

		// Append time index slot.
		offsets := make([]uint64, gmc.groupNum)
		var pos uint64
		var l int
		var n int
		for i, grp := range gmc.group {
			// NOTE(Alec), cannot use PadToNextByte here because it will generate an extra byte.
			// It will implicitly pad to next byte, only need to care for the WritePos.

			pos = uint64(grp.WritePos())
			pad := pos % 8
			if pad != 0 {
				pos += uint64(8) - pad
			}
			pos += uint64(seriesBases[i])
			l = bits.Len64(pos)
			if l > n {
				n = l
			}
			offsets[i] = pos
		}
		// Add the starting offsets for next slot.
		gmc.time[len(gmc.time)-1].AppendOffBits(offsets, n)
		gmc.time = append(gmc.time, c.time[c.tIdx[its[0].bIdx+1]:]...)
		gmc.startTimes = append(gmc.startTimes, c.startTimes[its[0].bIdx+1:]...)

		for i := range c.idx[its[0].bIdx+1:] {
			gmc.idx = append(gmc.idx, c.idx[its[0].bIdx+1+i] - (c.count - gmc.count))
			gmc.tIdx = append(gmc.tIdx, c.tIdx[its[0].bIdx+1+i] - oldTimeNum)
		}
	}
	gmc.nextStop = c.nextStop
	for i, it := range its {
		if !(its[0].bIdx == len(its[0].chunk.startTimes) - 1 && its[0].currentSample == its[0].numSamples) {
			gmc.group[i].bases = append(gmc.group[i].bases, c.group[i].bases[its[0].bIdx+1:]...)
		}
		gmc.group[i].b.stream = append(gmc.group[i].b.stream, c.group[i].b.stream[it.seriesOff>>3:]...)
		gmc.group[i].b.count = c.group[i].b.count
		gmc.group[i].leading = c.group[i].leading
		gmc.group[i].trailing = c.group[i].trailing
		gmc.group[i].last = c.group[i].last
		gmc.group[i].num = gmc.count
		gmc.group[i].seriesOff = seriesBases[i]
	}
	return gmc, c.count - gmc.count
}

type GM2Appender struct {
	chunk       *GroupMemoryChunk2
	bCount      int
	seriesBases []int
}

func (a *GM2Appender) Append(t int64, v float64) {}
func (a *GM2Appender) AppendGroup(t int64, vals []float64) {
	if len(a.chunk.group) == 0 {
		if a.chunk.groupNum == 0 {
			a.chunk.groupNum = len(vals)
		}
		for _, val := range vals {
			g := newGM2Group(val, a.chunk.tupleSize)
			g.Append(val)
			a.chunk.group = append(a.chunk.group, g)
		}
		gt := &GM2Time{}
		gt.AppendDeltaT(0)
		a.chunk.time = append(a.chunk.time, gt)

		// The nextStop is set when inserting the first sample.
		a.chunk.nextStop = nextSplitPoint(t)
		a.chunk.startTimes = append(a.chunk.startTimes, t)
		a.chunk.idx = append(a.chunk.idx, a.chunk.count)
		a.chunk.tIdx = append(a.chunk.tIdx, 0)

		a.chunk.count++
		a.bCount++
	} else if len(vals) == a.chunk.groupNum {
		// Create new tuple when 
		if t > a.chunk.nextStop {
			// New next stop point.
			a.chunk.nextStop = nextSplitPoint(t)
			a.chunk.startTimes = append(a.chunk.startTimes, t)
			a.chunk.idx = append(a.chunk.idx, a.chunk.count)
			a.chunk.tIdx = append(a.chunk.tIdx, len(a.chunk.time))

			// Append time index slot.
			offsets := make([]uint64, a.chunk.groupNum)
			var pos uint64
			var l int
			var n int
			for i, grp := range a.chunk.group {
				grp.PadToNextByte() // Pad to next byte because of the end of current block.
				pos = uint64(grp.WritePos()) + uint64(a.seriesBases[i])
				l = bits.Len64(pos)
				if l > n {
					n = l
				}
				offsets[i] = pos
			}
			// Add the starting offsets for next slot.
			a.chunk.time[len(a.chunk.time)-1].AppendOffBits(offsets, n)
			gt := &GM2Time{}
			gt.AppendDeltaT(t - a.chunk.startTimes[len(a.chunk.startTimes)-1])
			a.chunk.time = append(a.chunk.time, gt)

			for i, val := range vals {
				a.chunk.group[i].SetBase(val)
				a.chunk.group[i].Append(val)
			}
			a.bCount = 0
		} else if a.bCount % a.chunk.tupleSize == 0 {
			// Append time index slot.
			offsets := make([]uint64, a.chunk.groupNum)
			var pos uint64
			var l int
			var n int
			for i, grp := range a.chunk.group {
				pos = uint64(grp.WritePos()) + uint64(a.seriesBases[i])
				l = bits.Len64(pos)
				if l > n {
					n = l
				}
				offsets[i] = pos
			}
			// Add the starting offsets for next slot.
			a.chunk.time[len(a.chunk.time)-1].AppendOffBits(offsets, n)
			gt := &GM2Time{}
			gt.AppendDeltaT(t - a.chunk.startTimes[len(a.chunk.startTimes)-1])
			a.chunk.time = append(a.chunk.time, gt)

			for i, val := range vals {
				a.chunk.group[i].Append(val)
			}
			a.bCount = 0
		} else {
			a.chunk.time[len(a.chunk.time)-1].AppendDeltaT(t - a.chunk.startTimes[len(a.chunk.startTimes)-1])
			for i, val := range vals {
				a.chunk.group[i].Append(val)
			}
		}
		a.chunk.count += 1
		a.bCount++
	}
}

type GM2Iterator struct {
	chunk         *GroupMemoryChunk2
	seriesNum     int
	seriesOff     int
	seriesBase    int
	timeOff       int
	err           error
	leading       uint8
	trailing      uint8
	t             int64
	v             float64
	bIdx          int
	numSamples    int
	numTuples     int
	currentSample int
	timeNum       int
	timeNumInside int
}

func newGM2Iterator(chunk *GroupMemoryChunk2, timestamp int64, seriesNum int) *GM2Iterator {
	it := &GM2Iterator{chunk: chunk, t: math.MinInt64, seriesNum: seriesNum, seriesBase: chunk.group[seriesNum].seriesOff}
	if seriesNum >= it.chunk.groupNum {
		it.err = errors.Errorf("seriesNum >= it.chunk.groupNum")
		return it
	}
	it.seriesBase = chunk.group[seriesNum].seriesOff

	it.timeNum, it.currentSample, it.timeOff, it.err = it.findTime(timestamp)
	if it.err != nil {
		return it
	}

	it.v = it.chunk.group[it.seriesNum].bases[it.bIdx]
	_, it.err = it.findSeries()

	return it
}

func (it *GM2Iterator) Reset() {
	it.seriesOff = 0
	it.timeOff = 0
	it.err = nil
	it.leading = 0
	it.trailing = 0
	it.bIdx = 0
	it.numSamples = 0
	it.numTuples = 0
	it.currentSample = 0
	it.timeNum = 0
	it.timeNumInside = 0
}

func (it *GM2Iterator) At() (int64, float64) {
	return it.t, it.v
}

func (it *GM2Iterator) Next() bool {
	if it.err != nil || (it.bIdx == len(it.chunk.startTimes) - 1 && it.currentSample >= it.numSamples) {
		return false
	}
	if it.currentSample == it.numSamples {
		// Goto next tuple.
		it.timeNum++
		it.timeOff = 0
		it.timeNumInside = 0
		it.currentSample = 0
		it.bIdx++
		it.numSamples = it.chunk.idx[it.bIdx]
		it.numTuples = it.chunk.tIdx[it.bIdx]
		if it.bIdx == len(it.chunk.startTimes) - 1 {
			it.numSamples = it.chunk.count - it.numSamples
			it.numTuples = len(it.chunk.time) - it.numTuples
		} else {
			it.numSamples = it.chunk.idx[it.bIdx+1] - it.numSamples
			it.numTuples = it.chunk.tIdx[it.bIdx+1] - it.numTuples
		}
		it.v = it.chunk.group[it.seriesNum].bases[it.bIdx] // Reset the XOR base.

		// Pad the seriesOff to next byte.
		pad := it.seriesOff % 8
		if pad != 0 {
			it.seriesOff += 8 - pad
		}
	} else if it.timeNumInside % it.chunk.tupleSize == 0 && it.timeNumInside > 0 {
		// Goto next tuple.
		it.timeNum++
		it.timeNumInside = 0
		it.timeOff = 0
		it.v = it.chunk.group[it.seriesNum].bases[it.bIdx] // Reset the XOR base.
	}

	var delta int64
	var val   float64
	var n     int

	// Decode timestamp.
	delta, n, it.err = it.chunk.time[it.timeNum].ReadVarintAt(it.timeOff)
	if it.err != nil {
		return false
	}

	it.t = it.chunk.startTimes[it.bIdx] + delta
	it.timeOff += n << 3

	// Decode value.
	val, n, it.err = it.decode(it.seriesOff, it.v)
	if it.err != nil {
		return false
	}
	it.v = val
	it.seriesOff += n
	it.currentSample++
	it.timeNumInside++
	return true
}

func (it *GM2Iterator) Seek(t int64) bool {
	if it.t >= t {
		return true
	}
	if it.err != nil || (it.bIdx == len(it.chunk.startTimes) - 1 && it.currentSample >= it.numSamples) {
		return false
	}

	it.leading = 0
	it.trailing = 0
	it.timeNum, it.currentSample, it.timeOff, it.err = it.findTime(t)
	if it.err != nil {
		return false
	}
	it.v = it.chunk.group[it.seriesNum].bases[it.bIdx]
	it.seriesOff = 0
	_, it.err = it.findSeries()
	if it.err != nil {
		return false
	}
	return it.Next()
}

func (it *GM2Iterator) Err() error {
	return it.err
}

// <current time tuple, current sample inside block, offset>, find the first one >= time.
func (it *GM2Iterator) findTime(t int64) (int, int, int, error) {
	if it.chunk.count == 0 {
		return -1, -1, -1, ErrEmptyGMC
	}
	if t <= it.chunk.startTimes[0] {
		it.numSamples = it.chunk.idx[0]
		it.numTuples = it.chunk.tIdx[0]
		if len(it.chunk.startTimes) == 1 {
			it.numSamples = it.chunk.count - it.numSamples
			it.numTuples = len(it.chunk.time) - it.numTuples
		} else {
			it.numSamples = it.chunk.idx[1] - it.numSamples
			it.numTuples = it.chunk.tIdx[1] - it.numTuples
		}
		return 0, 0, 0, nil
	}

	// Binary search coarsely.
	it.bIdx = sort.Search(len(it.chunk.startTimes), func(i int) bool {
		return it.chunk.startTimes[i] > t
	})
	it.bIdx -= 1 // It won't be 0 because we have already filtered that.
	it.numSamples = it.chunk.idx[it.bIdx]
	it.numTuples = it.chunk.tIdx[it.bIdx]
	if it.bIdx == len(it.chunk.startTimes) - 1 {
		it.numSamples = it.chunk.count - it.numSamples
		it.numTuples = len(it.chunk.time) - it.numTuples
	} else {
		it.numSamples = it.chunk.idx[it.bIdx+1] - it.numSamples
		it.numTuples = it.chunk.tIdx[it.bIdx+1] - it.numTuples
	}

	t -= it.chunk.startTimes[it.bIdx] // Subtract the base.
	first := sort.Search(it.numTuples, func(i int) bool { // Binary search among tuples.
		temp, _, _ := it.chunk.time[it.chunk.tIdx[it.bIdx]+i].ReadVarintAt(0)
		return temp > t
	})
	if first != 0 {
		first -= 1
	}

	// NOTE(Alec), normally stop at the current tuple or the next tuple.
	for first < it.numTuples {
		pos := 0
		end := it.chunk.tupleSize
		if first == it.numTuples - 1 && it.numSamples % it.chunk.tupleSize != 0 {
			end = it.numSamples % it.chunk.tupleSize
		}
		for i := 0; i < end; i++ {
			temp, n, err := it.chunk.time[it.chunk.tIdx[it.bIdx]+first].ReadVarintAt(pos)
			if err != nil {
				return -1, -1, -1, err
			}
			if temp >= t {
				it.timeNumInside = i
				return it.chunk.tIdx[it.bIdx]+first, first * it.chunk.tupleSize + i, pos, nil
			}
			pos += n << 3
		}
		first += 1
	}

	// It goes to the next block.
	if it.bIdx != len(it.chunk.startTimes) - 1 {
		it.bIdx++
		it.numSamples = it.chunk.idx[it.bIdx]
		it.numTuples = it.chunk.tIdx[it.bIdx]
		if it.bIdx == len(it.chunk.startTimes) - 1 {
			it.numSamples = it.chunk.count - it.numSamples
			it.numTuples = len(it.chunk.time) - it.numTuples
		} else {
			it.numSamples = it.chunk.idx[it.bIdx+1] - it.numSamples
			it.numTuples = it.chunk.tIdx[it.bIdx+1] - it.numTuples
		}
		it.timeNumInside = 0
		it.currentSample = 0
		// It should be dirrectly returned from the first sample in the first tuple.
		return it.chunk.tIdx[it.bIdx], 0, 0, nil
	}
	return -1, it.numSamples, -1, ErrGMCOutOfBounds
}

// Return seriesOff.
func (it *GM2Iterator) findSeries() (int, error) {
	if it.timeNum == 0 {
		if it.currentSample == 0 {
			return 0, nil
		} else {
			for i := 0; i < it.currentSample % it.chunk.tupleSize; i++ {
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

	// If the time tuple is the starting tuple in current block,
	// the last tuple of the previous block may not be complete.
	end := it.chunk.tupleSize
	if it.currentSample < it.chunk.tupleSize {
		numSamples := it.chunk.idx[it.bIdx]
		if it.bIdx != 0 {
			numSamples = numSamples - it.chunk.idx[it.bIdx-1]
		}
		end = numSamples % it.chunk.tupleSize
		if end == 0 {
			end = it.chunk.tupleSize
		}
	}

	for i := 0; i < end; i++ { // Read tupleSize deltas.
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
	it.seriesOff = int(bits) - it.seriesBase
	if it.currentSample % it.chunk.tupleSize != 0 {
		var val float64
		for i := 0; i < it.currentSample % it.chunk.tupleSize; i++ {
			val, n, it.err = it.decode(it.seriesOff, it.v)
			if it.err != nil {
				return 0, it.err
			}
			it.v = val
			it.seriesOff += n
		}
	}
	return it.seriesOff, nil
}

// Return decoded value and decoded bits.
func (it *GM2Iterator) decode(offset int, last float64) (float64, int, error) {
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

// [mint, maxt].
func (it *GM2Iterator) numBetween(mint int64, maxt int64) int {
	if mint == math.MinInt64 {
		return it.chunk.count
	}

	timeNum1, currentSample1, _, err := it.findTime(mint)
	if err != nil || (timeNum1 == len(it.chunk.startTimes) - 1 && currentSample1 == it.chunk.count - it.chunk.idx[it.bIdx] - 1) {
		return 0
	}
	bIdx1 := it.bIdx
	timeNum2, currentSample2, timeOff2, err := it.findTime(mint)
	if err != nil {
		return 0
	}
	bIdx2 := it.bIdx

	delta, _, err := it.chunk.time[timeNum2].ReadVarintAt(timeOff2)
	if err != nil {
		return 0
	}
	if it.chunk.startTimes[bIdx2] + delta == maxt {
		return it.chunk.idx[bIdx2] - it.chunk.idx[bIdx1] + currentSample2 - currentSample1 + 1
	} else {
		return it.chunk.idx[bIdx2] - it.chunk.idx[bIdx1] + currentSample2 - currentSample1
	}
}

func (it *GM2Iterator) getValues(series int, d *[]float64, mint int64, maxt int64) {
	if series < it.chunk.groupNum {
		it.Reset()
		it.seriesNum = series
		
		it.timeNum, it.currentSample, it.timeOff, it.err = it.findTime(mint)
		if it.err != nil {
			return
		}
		it.v = it.chunk.group[it.seriesNum].bases[0]
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
