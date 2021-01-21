package chunkenc

import (
	"encoding/binary"
	// "fmt"
	"math"
	"math/bits"
	"sort"

	"github.com/naivewong/tsdb-group/encoding"
	"github.com/pkg/errors"
)

const (
	DefaultMedianSegment = 2 // NOTE(Alec), should be larger than 1.
	DefaultTupleSize = 4
)

// Timestamp
// ┌──────────────────────┬───────────────────┬────────────────────────┬───────────────────┬────────────────────────┬───────────────┬───┬───────────────┐
// │ #timestamp <uvarint> │ #series <uvarint> │ starting_time <varint> │ interval <varint> │ alignment_len <1 byte> │ diff_1 <bits> │...│ diff_n <bits> │
// └──────────────────────┴───────────────────┴────────────────────────┴───────────────────┴────────────────────────┴───────────────┴───┴───────────────┘
//
// Time series data
// ┌──────────────────────────────────────────┬───────────────────────────────────────────┐
// │ meidan_1 <8 bytes>                       │ meidan_2 <8 bytes>                        │
// ├──────────────────────────────────┬───────┴───────────────┬───┬───────────────────────┤
// │ block offsets alignment <1 byte> │ block 1 offset <bits> │...│ block n offset <bits> │
// ├──────────────────────────────────┴───────────────────────┴───┴───────────────────────┤
// │ ┌──────────────────────────────────────────────────────────────────────────────────┐ │
// │ │ Block 1 tuple alignment <1 byte>                                                 │ │
// │ ├──────────────────────────────────────────────────────────────────────────────────┤ │
// │ │         ┌────────┬────────┬────────┬────────┬────────┬────────┬────────┬────────┐│ │
// │ │ tuple 1 │ value1 │ value2 │ value3 │ value4 │ value5 │ value6 │ value7 │ value8 ││ │
// │ │         └────────┴────────┴────────┴────────┴────────┴────────┴────────┴────────┘│ │
// │ │                . . .                                                             │ │
// │ │ tuple n <alignment bits>                                                         │ │
// │ └──────────────────────────────────────────────────────────────────────────────────┘ │
// │                  . . .                                                               │
// │ ┌──────────────────────────────────────────────────────────────────────────────────┐ │
// │ │ Block n                                                                          │ │
// │ └──────────────────────────────────────────────────────────────────────────────────┘ │
// └──────────────────────────────────────────────────────────────────────────────────────┘
//
// GroupDiskChunk is the format for disk storage, which is converted from GroupMemoryChunk.
// As a result, GroupDiskChunk doesn't have Appender.
type GroupDiskChunk1 struct {
	b            *bstream
	segments     uint8 // Number of medians.
	alignments   uint8 // Tuple size.
	numTimestamp int
	numSeries    int
}

func NewGroupDiskChunk1() *GroupDiskChunk1 {
	return &GroupDiskChunk1{b: &bstream{}, segments: DefaultMedianSegment, alignments: DefaultTupleSize}
}

func NewGroupDiskChunk1FromBs(b *bstream) *GroupDiskChunk1 {
	x, n, err := b.ReadUvarintAt(0)
	if err != nil {
		return nil
	}
	numTimestamp := int(x)
	x, n, err = b.ReadUvarintAt(n << 3)
	if err != nil {
		return nil
	}
	return &GroupDiskChunk1{b: b, segments: DefaultMedianSegment, alignments: DefaultTupleSize, numTimestamp: numTimestamp, numSeries: int(x)}
}

// Encoding returns the encoding type.
func (c *GroupDiskChunk1) Encoding() Encoding {
	return EncGDC1
}

func (c *GroupDiskChunk1) Bytes() []byte {
	return c.b.bytes()
}

func (c *GroupDiskChunk1) Appender() (Appender, error) {
	return NewNopAppender(), nil
}
// Only to implement the interface here.
func (c *GroupDiskChunk1) Iterator() Iterator {
	return NewNopIterator()
}

func (c *GroupDiskChunk1) IteratorGroup(t int64, seriesNum int) Iterator {
	return newGD1Iterator(c, t, seriesNum)
}

func (c *GroupDiskChunk1) NumSamples() int {
	return c.numTimestamp * c.numSeries
}

func (c *GroupDiskChunk1) Size() int {
	return c.b.Size()
}

func (c *GroupDiskChunk1) Chunk() Chunk { return c }

func (c *GroupDiskChunk1) ConvertGMC1(gmc *GroupMemoryChunk1, chks []Meta, mint int64, maxt int64) error {
	if mint == math.MinInt64 {
		if gmc.Count() == 0 {
			return errors.Errorf("empty gmc mint:%d maxt:%d", mint, maxt)
		}
		lastTupleSize := gmc.count % gmc.tupleSize
		if lastTupleSize == 0 {
			lastTupleSize = gmc.tupleSize
		}
		var pos       int
		var n         int
		var err       error
		var deltaTime int64
		var interval  int64
		for i := 0; i < lastTupleSize; i++ {
			deltaTime, n, err = gmc.time[len(gmc.time) - 1].ReadVarintAt(pos)
			if err != nil {
				return err
			}
			pos += n << 3		
		}

		if gmc.count > 1 {
			interval = deltaTime / int64(gmc.count - 1) // make the number of intervals equal to the number of points.
		}

		// Write the min_time and max_time for each ChunkMeta.
		endTime := gmc.startTime + deltaTime
		for i := 0; i < len(chks); i++ {
			chks[i].MinTime = gmc.startTime
			chks[i].MaxTime = endTime
		}

		// Write the total number of timestamps.
		c.numTimestamp = gmc.count
		var temp [binary.MaxVarintLen64]byte
		encoded := binary.PutUvarint(temp[:], uint64(gmc.count))
		for i := 0; i < encoded; i++ {
			c.b.writeByte(temp[i])
		}

		// Write the total number of series.
		encoded = binary.PutUvarint(temp[:], uint64(gmc.groupNum))
		for i := 0; i < encoded; i++ {
			c.b.writeByte(temp[i])
		}

		// Write the starting time.
		encoded = binary.PutVarint(temp[:], gmc.startTime)
		for i := 0; i < encoded; i++ {
			c.b.writeByte(temp[i])
		}

		// Write the interval.
		encoded = binary.PutVarint(temp[:], interval)
		for i := 0; i < encoded; i++ {
			c.b.writeByte(temp[i])
		}

		// For each timestamp, compute the diff.
		diffs := make([]int64, 0, gmc.count)
		for i := 0; i < len(gmc.time) - 1; i++ {
			pos = 0
			for j := 0; j < gmc.tupleSize; j++ {
				deltaTime, n, err = gmc.time[i].ReadVarintAt(pos)
				if err != nil {
					return err
				}
				pos += n << 3
				diffs = append(diffs, deltaTime - int64(i * gmc.tupleSize + j) * interval)
			}
		}
		pos = 0
		for i := 0; i < lastTupleSize; i++ {
			deltaTime, n, err = gmc.time[len(gmc.time) - 1].ReadVarintAt(pos)
			if err != nil {
				return err
			}
			pos += n << 3
			diffs = append(diffs, deltaTime - int64((gmc.count - lastTupleSize) + i) * interval)
		}

		// Write the alignment.
		diffAlignment := encoding.MaxBits(diffs)
		c.b.writeByte(byte(diffAlignment))

		// Write the diffs.
		for i := 0; i < len(diffs); i++ {
			c.b.writeBits(uint64(diffs[i]), diffAlignment)
		}

		// Set the starting offset for the first time series.
		chks[0].SeriesRef = uint64(c.b.WritePos())

		// Write the Time series data.
		var maxOrders []int
		var minOrders []int
		for i := 0; i < (int(c.segments)+ 1) / 2; i++ {
			maxOrders = append(maxOrders, gmc.count * (i * 2 + 1) / (int(c.segments) * 2))
			minOrders = append(minOrders, gmc.count * (i * 2 + 1) / (int(c.segments) * 2))
		}

		it := gmc.gmcIterator()
		var values  []float64
		var medians []float64
		for i := 0; i < gmc.groupNum; i++ {
			values = values[:0]
			medians = medians[:0]
			it.getValues(i, &values, mint, maxt)
			if len(values) == 0 {
				return errors.Errorf("empty values after getValues()")
			}
			for _, order := range maxOrders {
				medians = append(medians, encoding.GetNthByMaxHeapFloat64(values, order))
			}
			for _, order := range minOrders {
				medians = append(medians, encoding.GetNthByMinHeapFloat64(values, order))
			}
			c.WriteValues(values, medians)
			// Record the starting offset of next series.
			if i < gmc.groupNum - 1 {
				chks[i + 1].SeriesRef = uint64(c.b.WritePos())
			}
		}
	} else {
		// First to convert the timestamp.
		it := gmc.IteratorGroup(mint, 0)

		var tempTimestamps []int64
		var interval       int64

		for it.Next() {
			t, _ := it.At()
			if t > maxt {
				break
			}
			tempTimestamps = append(tempTimestamps, t)
		}
		if len(tempTimestamps) == 0 {
			return errors.Errorf("empty gmc mint:%d maxt:%d", mint, maxt)
		}

		diffs := make([]int64, 0, len(tempTimestamps))
		if len(tempTimestamps) > 1 {
			interval = (tempTimestamps[len(tempTimestamps) - 1] - tempTimestamps[0]) / int64(len(tempTimestamps) - 1)
		}
		for i := 0; i < len(tempTimestamps); i++ {
			diffs = append(diffs, tempTimestamps[i] - int64(i) * interval - tempTimestamps[0])
		}

		// Write the min_time and max_time for each ChunkMeta.
		endTime := tempTimestamps[len(tempTimestamps) - 1]
		for i := 0; i < len(chks); i++ {
			chks[i].MinTime = tempTimestamps[0]
			chks[i].MaxTime = endTime
		}

		// Write the total number of series.
		c.numTimestamp = len(tempTimestamps)
		var temp [binary.MaxVarintLen64]byte
		encoded := binary.PutUvarint(temp[:], uint64(len(tempTimestamps)))
		for i := 0; i < encoded; i++ {
			c.b.writeByte(temp[i])
		}

		// Write the total number of series.
		encoded = binary.PutUvarint(temp[:], uint64(gmc.groupNum))
		for i := 0; i < encoded; i++ {
			c.b.writeByte(temp[i])
		}

		// Write the starting time.
		encoded = binary.PutVarint(temp[:], tempTimestamps[0])
		for i := 0; i < encoded; i++ {
			c.b.writeByte(temp[i])
		}

		// Write the interval.
		encoded = binary.PutVarint(temp[:], interval)
		for i := 0; i < encoded; i++ {
			c.b.writeByte(temp[i])
		}

		// Write the alignment.
		diffAlignment := encoding.MaxBits(diffs)
		c.b.writeByte(byte(diffAlignment))

		// Write the diffs.
		for i := 0; i < len(diffs); i++ {
			c.b.writeBits(uint64(diffs[i]), diffAlignment)
		}

		// Set the starting offset for the first time series.
		chks[0].SeriesRef = uint64(c.b.WritePos())

		// Write the Time series data.
		var maxOrders []int
		var minOrders []int
		for i := 0; i < (int(c.segments) + 1) / 2; i++ {
			maxOrders = append(maxOrders, c.numTimestamp * (i * 2 + 1) / (int(c.segments) * 2))
			minOrders = append(minOrders, c.numTimestamp * (i * 2 + 1) / (int(c.segments) * 2))
		}

		it2 := gmc.gmcIterator()
		var values  []float64
		var medians []float64
		for i := 0; i < gmc.groupNum; i++ {
			values = values[:0]
			medians = medians[:0]
			it2.getValues(i, &values, mint, maxt)
			if len(values) == 0 {
				return errors.Errorf("empty values after getValues()")
			}
			for _, order := range maxOrders {
				medians = append(medians, encoding.GetNthByMaxHeapFloat64(values, order))
			}
			for _, order := range minOrders {
				medians = append(medians, encoding.GetNthByMinHeapFloat64(values, order))
			}
			c.WriteValues(values, medians)
			// Record the starting offset of next series.
			if i < gmc.groupNum - 1 {
				chks[i + 1].SeriesRef = uint64(c.b.WritePos())
			}
		}
	}
	c.numSeries = len(chks)
	return nil
}

func (c *GroupDiskChunk1) ConvertGMC2(gmc *GroupMemoryChunk2, chks []Meta, mint int64, maxt int64) error {
	if gmc.count == 0 {
		return errors.Errorf("empty gmc mint:%d maxt:%d", mint, maxt)
	}
	it1 := newGM2Iterator(gmc, mint, 0)
	it2 := newGM2Iterator(gmc, maxt, 0)
	numSamples := gmc.idx[it2.bIdx] - gmc.idx[it1.bIdx] + it2.currentSample - it1.currentSample + 1
	if !it1.Next() {
		return errors.Errorf("empty gmc mint:%d maxt:%d", mint, maxt)
	}
	startTime, _ := it1.At()

	var deltaTime int64
	var interval  int64
	var endTime   int64
	var pos       int
	var n         int
	var err       error
	if !it2.Next() {
		numSamples-- // Exclude the current it2 sample.
		lastTupleSize := (gmc.count - gmc.idx[len(gmc.idx)-1]) % gmc.tupleSize
		if lastTupleSize == 0 {
			lastTupleSize = gmc.tupleSize
		}
		for i := 0; i < lastTupleSize; i++ {
			deltaTime, n, err = gmc.time[len(gmc.time) - 1].ReadVarintAt(pos)
			if err != nil {
				return err
			}
			pos += n << 3		
		}
		if numSamples > 1 {
			interval = (gmc.startTimes[len(gmc.startTimes)-1] + deltaTime - startTime) / int64(numSamples - 1)
		} else {
			interval = (gmc.startTimes[len(gmc.startTimes)-1] + deltaTime - startTime) / int64(numSamples)
		}
	} else {
		t, _ := it2.At()

		if t > maxt {
			numSamples-- // Exclude the current it2 sample.
			lastTupleSize := int(c.alignments)
			if it2.currentSample % int(c.alignments) == 1 {
				// Need to go to the previous time tuple.
				if it2.currentSample == 1 {
					// Need to go to previous block.
					lastTupleSize = gmc.idx[it2.bIdx]
					if it2.bIdx != 0 {
						lastTupleSize -= gmc.idx[it2.bIdx-1]
					}
					lastTupleSize %= int(c.alignments)
				}
			}
			pos = 0
			for i := 0; i < lastTupleSize; i++ {
				deltaTime, n, err = gmc.time[it2.timeNum-1].ReadVarintAt(pos)
				if err != nil {
					return err
				}
				pos += n << 3		
			}
			if it2.currentSample == 1 {
				endTime = gmc.startTimes[it2.bIdx-1] + deltaTime
			} else {
				endTime = gmc.startTimes[it2.bIdx] + deltaTime
			}
		} else {
			endTime = t
		}
		if numSamples > 1 {
			interval = (endTime - startTime) / int64(numSamples - 1)
		} else {
			interval = (endTime - startTime) / int64(numSamples)
		}
	}

	// Write the min_time and max_time for each ChunkMeta.
	for i := 0; i < len(chks); i++ {
		chks[i].MinTime = startTime
		chks[i].MaxTime = endTime
	}

	// Write the total number of timestamps.
	c.numTimestamp = numSamples
	var temp [binary.MaxVarintLen64]byte
	encoded := binary.PutUvarint(temp[:], uint64(numSamples))
	for i := 0; i < encoded; i++ {
		c.b.writeByte(temp[i])
	}

	// Write the total number of series.
	encoded = binary.PutUvarint(temp[:], uint64(gmc.groupNum))
	for i := 0; i < encoded; i++ {
		c.b.writeByte(temp[i])
	}

	// Write the starting time.
	encoded = binary.PutVarint(temp[:], startTime)
	for i := 0; i < encoded; i++ {
		c.b.writeByte(temp[i])
	}

	// Write the interval.
	encoded = binary.PutVarint(temp[:], interval)
	for i := 0; i < encoded; i++ {
		c.b.writeByte(temp[i])
	}

	// For each timestamp, compute the diff.
	diffs := make([]int64, 0, numSamples)
	diffs = append(diffs, 0)
	idx := 1
	for it1.Next() {
		t, _ := it1.At()
		if t > maxt {
			break
		}
		diffs = append(diffs, t - startTime - int64(idx) * interval)
		idx++
	}

	// Write the alignment.
	diffAlignment := encoding.MaxBits(diffs)
	c.b.writeByte(byte(diffAlignment))

	// Write the diffs.
	for i := 0; i < len(diffs); i++ {
		c.b.writeBits(uint64(diffs[i]), diffAlignment)
	}

	// Set the starting offset for the first time series.
	chks[0].SeriesRef = uint64(c.b.WritePos())

	// Write the Time series data.
	var maxOrders []int
	var minOrders []int
	for i := 0; i < (int(c.segments)+ 1) / 2; i++ {
		maxOrders = append(maxOrders, gmc.count * (i * 2 + 1) / (int(c.segments) * 2))
		minOrders = append(minOrders, gmc.count * (i * 2 + 1) / (int(c.segments) * 2))
	}

	it := gmc.gmcIterator()
	var values  []float64
	var medians []float64
	for i := 0; i < gmc.groupNum; i++ {
		values = values[:0]
		medians = medians[:0]
		it.getValues(i, &values, mint, maxt)
		if len(values) == 0 {
			return errors.Errorf("empty values after getValues()")
		}
		for _, order := range maxOrders {
			medians = append(medians, encoding.GetNthByMaxHeapFloat64(values, order))
		}
		for _, order := range minOrders {
			medians = append(medians, encoding.GetNthByMinHeapFloat64(values, order))
		}
		c.WriteValues(values, medians)
		// Record the starting offset of next series.
		if i < gmc.groupNum - 1 {
			chks[i + 1].SeriesRef = uint64(c.b.WritePos())
		}
	}
	c.numSeries = len(chks)
	return nil
}

func (c *GroupDiskChunk1) WriteValues(values []float64, medians []float64) {
	for _, median := range medians {
		c.b.writeBits(math.Float64bits(median), 64)
	}

	var tuples []*bstream
	var temp [binary.MaxVarintLen64]byte
	indicatorLen := bits.Len(uint(len(medians) - 1))
	numTuple := (len(values) + int(c.alignments) - 1) / int(c.alignments)
	tupleLen := int(c.alignments)
	numBlock := (numTuple+9)/10
	maxSize := make([]int, numBlock)
	sizeCount := numBlock*48 // The offsets bits (max 32) plus the alignment (max 16) of the block tuple alignment.
	segIdx := 0
	cachedXors := make([]uint64, 0, c.alignments) // The cached XOR results in case 2.

	for i := 0; i < numTuple; i++ {
		if i % 10 == 0 && i > 0 {
			// Next block.
			sizeCount += maxSize[segIdx] * 10
			segIdx++
		}
		tempBS := &bstream{}
		var closest int
		for j, median := range medians[1:] {
			if math.Abs(median - values[i * int(c.alignments)]) < math.Abs(medians[closest] - values[i * int(c.alignments)]) {
				closest = j
			}
		}

		// Write the indicator for the first value.
		tempBS.writeBits(uint64(closest), indicatorLen)

		leading := uint8(0xff)
		trailing := uint8(0)
		mlz := uint8(64)
		mtz := uint8(64)
		last := medians[closest]

		// Determine if the current tuple size.
		if i == numTuple - 1 && len(values) % int(c.alignments) != 0 {
			tupleLen = len(values) % int(c.alignments)
		}

		// Clean cached XORs.
		cachedXors = cachedXors[:0]

		for j := 0; j < tupleLen; j++ {
			delta := math.Float64bits(values[i * int(c.alignments) + j]) ^ math.Float64bits(last)
			if delta == 0 {
				if len(cachedXors) > 0 {
					// Clean.
					if len(cachedXors) * int(mlz + mtz - leading - trailing) > 11 {
						leading = mlz
						trailing = mtz
						tempBS.writeBit(one) // First control bit.
						tempBS.writeBit(one) // Seocnd control bit.
						tempBS.writeBits(uint64(leading), 5) // Write leading zero num.

						sigbits := 64 - int(leading) - int(trailing)
						tempBS.writeBits(uint64(sigbits), 6)         // Write significant bits num.
						tempBS.writeBits(cachedXors[0] >> trailing, sigbits) // Write significant bits.

						cachedXors = cachedXors[1:]
					}
					for _, xor := range cachedXors {
						tempBS.writeBit(one) // First control bit.
						tempBS.writeBit(zero) // Second control bit.
						tempBS.writeBits(xor >> trailing, 64 - int(leading) - int(trailing))
					}
					cachedXors = cachedXors[:0]
					mlz = 64
					mtz = 64
				}
				tempBS.writeBit(zero) // First control bit.
				continue
			}

			lz := uint8(bits.LeadingZeros64(delta))
			tz := uint8(bits.TrailingZeros64(delta))

			// Clamp number of leading zeros to avoid overflow when encoding.
			if lz >= 32 {
				lz = 31
			}
			if leading != 0xff && lz >= leading && tz >= trailing {
				if lz < mlz {
					mlz = lz
				}
				if tz < mtz {
					mtz = tz
				}
				cachedXors = append(cachedXors, delta)
			} else {
				if len(cachedXors) > 0 {
					// Clean.
					if len(cachedXors) * int(mlz + mtz - leading - trailing) > 11 {
						leading = mlz
						trailing = mtz
						tempBS.writeBit(one) // First control bit.
						tempBS.writeBit(one) // Seocnd control bit.
						tempBS.writeBits(uint64(leading), 5) // Write leading zero num.

						sigbits := 64 - int(leading) - int(trailing)
						tempBS.writeBits(uint64(sigbits), 6)         // Write significant bits num.
						tempBS.writeBits(cachedXors[0] >> trailing, sigbits) // Write significant bits.

						cachedXors = cachedXors[1:]
					}
					for _, xor := range cachedXors {
						tempBS.writeBit(one) // First control bit.
						tempBS.writeBit(zero) // Second control bit.
						tempBS.writeBits(xor >> trailing, 64 - int(leading) - int(trailing))
					}
					cachedXors = cachedXors[:0]
					mlz = 64
					mtz = 64
				}
				leading = lz
				trailing = tz

				tempBS.writeBit(one) // First control bit.
				if j != 0 {
					tempBS.writeBit(one)    // Seocnd control bit.
				}
				tempBS.writeBits(uint64(lz), 5) // Write leading zero num.

				sigbits := 64 - int(leading) - int(trailing)
				tempBS.writeBits(uint64(sigbits), 6)         // Write significant bits num.
				tempBS.writeBits(delta >> trailing, sigbits) // Write significant bits.
			}
			last = values[i * int(c.alignments) + j]
		}
		if len(cachedXors) > 0 {
			// Clean.
			if len(cachedXors) * int(mlz + mtz - leading - trailing) > 11 {
				leading = mlz
				trailing = mtz
				tempBS.writeBit(one) // First control bit.
				tempBS.writeBit(one) // Seocnd control bit.
				tempBS.writeBits(uint64(leading), 5) // Write leading zero num.

				sigbits := 64 - int(leading) - int(trailing)
				tempBS.writeBits(uint64(sigbits), 6)         // Write significant bits num.
				tempBS.writeBits(cachedXors[0] >> trailing, sigbits) // Write significant bits.

				cachedXors = cachedXors[1:]
			}
			for _, xor := range cachedXors {
				tempBS.writeBit(one) // First control bit.
				tempBS.writeBit(zero) // Second control bit.
				tempBS.writeBits(xor >> trailing, 64 - int(leading) - int(trailing))
			}
			cachedXors = cachedXors[:0]
		}
		if maxSize[segIdx] < tempBS.WritePos() {
			maxSize[segIdx] = tempBS.WritePos()
		}
		tuples = append(tuples, tempBS)
	}

	sizeCount += maxSize[segIdx] * tupleLen

	sizeCount = bits.Len(uint(sizeCount))
	// Write alignment of block starting offsets.
	c.b.writeByte(byte(sizeCount))
	// Write first block's starting offset.
	c.b.writeBits(uint64(0), sizeCount)
	last := 0
	for i := 0; i < numBlock - 1; i++ {
		last += maxSize[i] * 10 + binary.PutUvarint(temp[:], uint64(maxSize[i])) * 8
		c.b.writeBits(uint64(last), sizeCount)
	}

	for i := 0; i < numBlock - 1; i++ {
		// Write tuple alignment length.
		encoded := binary.PutUvarint(temp[:], uint64(maxSize[i]))
		for i := 0; i < encoded; i++ {
			c.b.writeByte(temp[i])
		}
		for j := i * 10; j < i * 10 + 10; j++ {
			c.b.WriteStream(tuples[j], tuples[j].WritePos())
			if tuples[j].WritePos() < maxSize[i] {
				c.b.WritePadding(maxSize[i] - tuples[j].WritePos())
			}
		}
	}

	// Write tuple alignment length.
	encoded := binary.PutUvarint(temp[:], uint64(maxSize[len(maxSize) - 1]))
	for i := 0; i < encoded; i++ {
		c.b.writeByte(temp[i])
	}

	for j := (numBlock - 1) * 10; j < numTuple; j++ {
		c.b.WriteStream(tuples[j], tuples[j].WritePos())
		if tuples[j].WritePos() < maxSize[len(maxSize) - 1] {
			c.b.WritePadding(maxSize[len(maxSize) - 1] - tuples[j].WritePos())
		}
	}
}

type GD1Iterator struct {
	chunk          *GroupDiskChunk1
	seriesOff      int
	seriesStartOff int
	blockStartOff  int
	blockSize      int
	blockOffLen    int
	tupleIdx       int
	insideTuple    int
	index          int
	timeOff        int
	timeStartOff   int
	err            error
	leading        uint8
	trailing       uint8
	t              int64
	v              float64
	medians        []float64
	tAlign         int
	vAlign         int
	maxDiff        int64
	minDiff        int64
	startTime      int64
	interval       int64
	indicatorLen   int
}

func newGD1Iterator(chunk *GroupDiskChunk1, time int64, seriesOff int) *GD1Iterator {
	it := &GD1Iterator{chunk: chunk, seriesOff: seriesOff, seriesStartOff: seriesOff, t: math.MinInt64, blockSize: int(chunk.alignments) * 10}
	it.err = it.findTime(time)
	if it.err != nil {
		return it
	}
	it.err = it.findSeries(seriesOff)

	return it
}

func (it *GD1Iterator) At() (int64, float64) {
	return it.t, it.v
}

func (it *GD1Iterator) Next() bool {
	if it.err != nil || it.index >= it.chunk.numTimestamp {
		return false
	}

	// Read timestamp.
	var x uint64
	x, it.err = it.chunk.b.ReadBitsAt(it.timeOff, it.tAlign)
	if it.err != nil {
		return false
	}
	it.t = it.startTime + int64(it.index) * it.interval + encoding.FromBits(x, it.tAlign)
	it.timeOff += it.tAlign

	// Read value.
	if it.insideTuple % int(it.chunk.alignments) == 0 {
		if it.insideTuple > 0 {
			it.tupleIdx++
			it.insideTuple = 0
		}
		if it.tupleIdx == 10 {
			// Next block.
			it.seriesOff = it.blockStartOff + it.vAlign * 10
			var n int
			var x uint64
			x, n, it.err = it.chunk.b.ReadUvarintAt(it.seriesOff)
			it.vAlign = int(x)
			it.seriesOff += n << 3
			it.blockStartOff = it.seriesOff
			it.tupleIdx = 0
		}

		// The current value is the first element in the tuple.
		it.seriesOff = it.blockStartOff + it.tupleIdx * it.vAlign
		it.err = it.readValue(true)
		if it.err != nil {
			return false
		}
		it.insideTuple++
		it.index++
		return true
	}

	it.err = it.readValue(false)
	if it.err != nil {
		return false
	}
	it.insideTuple++
	it.index++
	return true
}

func (it *GD1Iterator) Seek(x int64) bool {
	if it.t >= x {
		return true
	}
	if it.err != nil {
		return false
	}
	it.reset()
	it.findTimeHelper(x)
	if it.err != nil {
		return false
	}
	it.err = it.findSeriesHelper()
	if it.err != nil {
		return false
	}
	return it.Next()
}

func (it *GD1Iterator) Err() error {
	return it.err
}

func (it *GD1Iterator) reset() {
	it.index = 0
	it.leading = 0
	it.trailing = 0
	it.timeOff = it.timeStartOff
	it.seriesOff = it.seriesStartOff
	it.insideTuple = 0
}

func (it *GD1Iterator) findTime(t int64) error {
	it.timeOff = 0
	var n int
	var a byte
	var x uint64
	x, n, it.err = it.chunk.b.ReadUvarintAt(it.timeOff) // #timestamps
	if it.err != nil {
		return it.err
	}
	it.chunk.numTimestamp = int(x)
	it.timeOff += n << 3
	x, n, it.err = it.chunk.b.ReadUvarintAt(it.timeOff) // #series.
	if it.err != nil {
		return it.err
	}
	it.chunk.numSeries = int(x)
	it.timeOff += n << 3
	it.startTime, n, it.err = it.chunk.b.ReadVarintAt(it.timeOff) // starting time.
	if it.err != nil {
		return it.err
	}
	it.timeOff += n << 3
	it.interval, n, it.err = it.chunk.b.ReadVarintAt(it.timeOff) // interval.
	if it.err != nil {
		return it.err
	}
	it.timeOff += n << 3
	a, it.err = it.chunk.b.ReadByteAt(it.timeOff) // timestamp diff alignment length.
	if it.err != nil {
		return it.err
	}
	it.tAlign = int(a)
	it.timeOff += 8
	it.maxDiff = encoding.MaxFromBitsLen(it.tAlign)
	it.minDiff = encoding.MinFromBitsLen(it.tAlign)
	it.timeStartOff = it.timeOff
	it.findTimeHelper(t)
	return it.err
}

func (it *GD1Iterator) findTimeHelper(t int64) {
	if it.interval != 0 {
		first := int((t + it.minDiff - it.startTime) / it.interval - 1)
		if first < 0 {
			first = 0
		}
		if first > it.chunk.numTimestamp {
			first = it.chunk.numTimestamp
		}
		var c int // Range for the binary search.
		if t >= it.startTime {
			c = int((t + it.maxDiff - it.startTime) / it.interval) - first + 1
			if c > it.chunk.numTimestamp - first {
				c = it.chunk.numTimestamp - first
			}
		} else {
			c = 1
		}

		sto := it.timeOff + first * it.tAlign
		st := it.startTime + int64(first) * it.interval
		var temp uint64
		i := sort.Search(c, func(i int) bool { // Binary search among tuples.
			temp, it.err = it.chunk.b.ReadBitsAt(sto + i * it.tAlign, it.tAlign)
			return st + it.interval * int64(i) + encoding.FromBits(temp, it.tAlign) >= t
		})
		it.index = first + i
		it.timeOff += it.index * it.tAlign
	} else {
		if it.startTime >= t {
			it.index = 0
		} else {
			it.err = errors.Errorf("cannot find time")
		}
	}
}

func (it *GD1Iterator) findSeries(offset int) error {
	it.seriesOff = offset

	// Read medians.
	var x uint64
	for i := 0; i < int(it.chunk.segments); i++ {
		x, it.err = it.chunk.b.ReadBitsAt(it.seriesOff, 64)
		if it.err != nil {
			return it.err
		}
		it.medians = append(it.medians, math.Float64frombits(x))
		it.seriesOff += 64
	}

	// Read block starting offsets length alignment.
	var offLen byte
	offLen, it.err = it.chunk.b.ReadByteAt(it.seriesOff)
	if it.err != nil {
		return it.err
	}
	it.blockOffLen = int(offLen)
	it.seriesOff += 8
	it.seriesStartOff = it.seriesOff

	return it.findSeriesHelper()
}

func (it *GD1Iterator) findSeriesHelper() error {
	// Read starting block offset.
	numBlock := (it.chunk.numTimestamp + it.blockSize - 1) / it.blockSize
	blockIdx := it.index / it.blockSize
	var blockOff uint64
	blockOff, it.err = it.chunk.b.ReadBitsAt(it.seriesOff + it.blockOffLen * blockIdx, it.blockOffLen)
	if it.err != nil {
		return it.err
	}
	it.seriesOff += it.blockOffLen * numBlock + int(blockOff)

	// Calculate the indicator length.
	it.indicatorLen = bits.Len(uint(len(it.medians) - 1))

	// Read current block's tuple alignment.
	var n int
	var x uint64
	x, n, it.err = it.chunk.b.ReadUvarintAt(it.seriesOff)
	it.vAlign = int(x)
	it.seriesOff += n << 3
	it.blockStartOff = it.seriesOff

	// Go to the corresponding tuple.
	sampleIdx := it.index % it.blockSize
	it.tupleIdx = sampleIdx / int(it.chunk.alignments)
	it.seriesOff += it.tupleIdx * it.vAlign

	it.insideTuple = sampleIdx % int(it.chunk.alignments)
	if it.insideTuple == 0 {
		return nil;
	}
	// Read the first element of current tuple.
	err := it.readValue(true)
	if err != nil {
		return err
	}
	for i := 0; i < it.index % int(it.chunk.alignments) - 1; i++ {
		err = it.readValue(false)
		if err != nil {
			return err
		}
	}
	return nil
}

func (it *GD1Iterator) readValue(first bool) error {
	var cb bit
	var x  uint64
	if first {
		x, it.err = it.chunk.b.ReadBitsAt(it.seriesOff, it.indicatorLen)
		if it.err != nil {
			return it.err
		}
		it.v = it.medians[int(x)]
		it.seriesOff += it.indicatorLen

		cb, it.err = it.chunk.b.ReadBitAt(it.seriesOff) // First control bit
		if it.err != nil {
			return it.err
		}
		it.seriesOff += 1

		if cb != zero {
			x, it.err = it.chunk.b.ReadBitsAt(it.seriesOff, 5) // Leading zero.
			if it.err != nil {
				return it.err
			}
			it.seriesOff += 5
			it.leading = uint8(x)

			x, it.err = it.chunk.b.ReadBitsAt(it.seriesOff, 6) // Significant bits.
			if it.err != nil {
				return it.err
			}
			it.seriesOff += 6
			// 0 significant bits here means we overflowed and we actually need 64; see comment in encoder
			if x == 0 {
				x = 64
			}
			it.trailing = 64 - it.leading - uint8(x)

			x, it.err = it.chunk.b.ReadBitsAt(it.seriesOff, int(64 - it.leading - it.trailing)) // XOR bits.
			if it.err != nil {
				return it.err
			}
			it.seriesOff += int(64 - it.leading - it.trailing)

			vbits := math.Float64bits(it.v)
			vbits ^= x << it.trailing
			it.v = math.Float64frombits(vbits)
		}
		return nil
	}
	cb, it.err = it.chunk.b.ReadBitAt(it.seriesOff) // First control bit
	if it.err != nil {
		return it.err
	}
	it.seriesOff += 1

	if cb != zero {
		cb, it.err = it.chunk.b.ReadBitAt(it.seriesOff) // Second control bit
		if it.err != nil {
			return it.err
		}
		it.seriesOff += 1
		if cb != zero {
			x, it.err = it.chunk.b.ReadBitsAt(it.seriesOff, 5) // Leading zero.
			if it.err != nil {
				return it.err
			}
			it.seriesOff += 5
			it.leading = uint8(x)

			x, it.err = it.chunk.b.ReadBitsAt(it.seriesOff, 6) // Significant bits.
			if it.err != nil {
				return it.err
			}
			it.seriesOff += 6
			// 0 significant bits here means we overflowed and we actually need 64; see comment in encoder
			if x == 0 {
				x = 64
			}
			it.trailing = 64 - it.leading - uint8(x)
		}

		x, it.err = it.chunk.b.ReadBitsAt(it.seriesOff, int(64 - it.leading - it.trailing)) // XOR bits.
		if it.err != nil {
			return it.err
		}
		it.seriesOff += int(64 - it.leading - it.trailing)

		vbits := math.Float64bits(it.v)
		vbits ^= x << it.trailing
		it.v = math.Float64frombits(vbits)
	}
	return nil
}
