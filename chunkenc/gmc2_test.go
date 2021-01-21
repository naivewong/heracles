package chunkenc

import (
	// "fmt"
	// "math"
	"math/rand"
	"testing"

	"github.com/naivewong/tsdb-group/testutil"
)

func TestGMC2IncompleteTuple(test *testing.T) {
	timestamps := []int64{1092, 2344, 4444, 6666, 6667, 7777, 7778, 7779, 7780, 7781, 7782}
	values := []float64{0.3, 9, 241, 23.173, 1222.9874, 2131.22, 12.2, 22.2, 33.5, 43.8, 88.9}

	gmc := NewGroupMemoryChunk2(1)
	app, _ := gmc.Appender()
	for i := 0; i < len(timestamps); i++ {
		app.AppendGroup(timestamps[i], []float64{values[i]})
	}

	var t int64
	var v float64

	// Test Next().
	it := gmc.IteratorGroup(7780, 0)
	i := 8
	for it.Next() {
		t, v = it.At()
		testutil.Equals(test, timestamps[i], t)
		testutil.Equals(test, values[i], v)
		i += 1
	}
	testutil.Equals(test, len(timestamps), i)
}

func TestGMC2OneSeries(test *testing.T) {
	timestamps := []int64{1092, 2344, 4444, 6666, 6667, 7777, 7778, 7779, 7780, 7781, 7782}
	values := []float64{0.3, 9, 241, 23.173, 1222.9874, 2131.22, 12.2, 22.2, 33.5, 43.8, 88.9}

	gmc := NewGroupMemoryChunk2(1)
	app, _ := gmc.Appender()
	for i := 0; i < len(timestamps); i++ {
		app.AppendGroup(timestamps[i], []float64{values[i]})
	}

	var t int64
	var v float64

	// Test Next().
	it := gmc.IteratorGroup(0, 0)
	i := 0
	for it.Next() {
		t, v = it.At()
		testutil.Equals(test, timestamps[i], t)
		testutil.Equals(test, values[i], v)
		i += 1
	}
	testutil.Equals(test, len(timestamps), i)

	// Test Seek().
	it = gmc.IteratorGroup(0, 0)
	i = 1
	testutil.Equals(test, true, it.Seek(1100))
	t, v = it.At()
	testutil.Equals(test, timestamps[i], t)
	testutil.Equals(test, values[i], v)
	i += 1
	for it.Next() {
		t, v = it.At()
		testutil.Equals(test, timestamps[i], t)
		testutil.Equals(test, values[i], v)
		i += 1
	}
	testutil.Equals(test, len(timestamps), i)

	// Test Seek().
	it = gmc.IteratorGroup(0, 0)
	i = 9
	testutil.Equals(test, true, it.Seek(7781))
	t, v = it.At()
	testutil.Equals(test, timestamps[i], t)
	testutil.Equals(test, values[i], v)
	i += 1
	for it.Next() {
		t, v = it.At()
		testutil.Equals(test, timestamps[i], t)
		testutil.Equals(test, values[i], v)
		i += 1
	}
	testutil.Equals(test, len(timestamps), i)

	it = gmc.IteratorGroup(8900, 0)
	testutil.Equals(test, false, it.Next())
}

func TestGMC2TwoSeries(test *testing.T) {
	timestamps := []int64{1092, 2344, 4444, 6666, 6667, 7777, 7778, 7779, 7780, 7781, 7782}
	values := [][]float64{
		[]float64{0.3, 9, 241, 23.173, 1222.9874, 2131.22, 12.2, 22.2, 33.5, 43.8, 88.9},
		[]float64{0.74, 212.66, 9863.2, 443.019, 23, 654.999, 12.2, 22.2, 33.5, 43.8, 88.9},
	}

	gmc := &GroupMemoryChunk2{groupNum: 2, tupleSize: 4}
	app, _ := gmc.Appender()
	for i := 0; i < len(timestamps); i++ {
		app.AppendGroup(timestamps[i], []float64{values[0][i], values[1][i]})
	}

	var t int64
	var v float64

	// Test Next() for the first series.
	it := gmc.IteratorGroup(0, 0)
	i := 0
	for it.Next() {
		t, v = it.At()
		testutil.Equals(test, timestamps[i], t)
		testutil.Equals(test, values[0][i], v)
		i += 1
	}
	testutil.Equals(test, len(timestamps), i)

	// Test Next() for the second series.
	it = gmc.IteratorGroup(0, 1)
	i = 0
	for it.Next() {
		t, v = it.At()
		testutil.Equals(test, timestamps[i], t)
		testutil.Equals(test, values[1][i], v)
		i += 1
	}
	testutil.Equals(test, len(timestamps), i)

	// Test Next() for the first series start from 6667.
	it = gmc.IteratorGroup(6667, 0)
	i = 4
	for it.Next() {
		t, v = it.At()
		testutil.Equals(test, timestamps[i], t)
		testutil.Equals(test, values[0][i], v)
		i += 1
	}
	testutil.Equals(test, len(timestamps), i)

	it = gmc.IteratorGroup(8900, 0)
	testutil.Equals(test, false, it.Next())
}

func TestGMC2TwoSeriesV2(test *testing.T) {
	gmc := NewGroupMemoryChunk2(2)
	app, _ := gmc.Appender()
	for i := 0; i < 4000; i += 5 {
		app.AppendGroup(int64(i), []float64{float64(i), float64(i)})
	}

	it := gmc.IteratorGroup(2500, 0)
	i := 2500
	for it.Next() {
		t, v := it.At()
		testutil.Equals(test, int64(i), t)
		testutil.Equals(test, float64(i), v)
		i += 5
	}
	testutil.Equals(test, 4000, i)
}

func TestGMC2ManyPoints(test *testing.T) {
	var timestamps []int64
	var values []float64
	for i := 0; i < 10000; i++ {
		timestamps = append(timestamps, int64(i*1000))
		values = append(values, rand.Float64())
	}

	gmc := &GroupMemoryChunk2{groupNum: 1, tupleSize: 4}
	app, _ := gmc.Appender()
	for i := 0; i < len(timestamps); i++ {
		app.AppendGroup(timestamps[i], []float64{values[i]})
	}

	var t int64
	var v float64

	it := gmc.IteratorGroup(0, 0)
	i := 0
	for it.Next() {
		t, v = it.At()
		testutil.Equals(test, timestamps[i], t)
		testutil.Equals(test, values[i], v)
		i += 1
	}
	testutil.Equals(test, len(timestamps), i)

	it = gmc.IteratorGroup(2000000, 0)
	i = 2000
	for it.Next() {
		t, v = it.At()
		testutil.Equals(test, timestamps[i], t)
		testutil.Equals(test, values[i], v)
		i += 1
	}
	testutil.Equals(test, len(timestamps), i)

	it = gmc.IteratorGroup(5000000, 0)
	i = 5000
	for it.Next() {
		t, v = it.At()
		testutil.Equals(test, timestamps[i], t)
		testutil.Equals(test, values[i], v)
		i += 1
	}
	testutil.Equals(test, len(timestamps), i)

	it = gmc.IteratorGroup(7777000, 0)
	i = 7777
	for it.Next() {
		t, v = it.At()
		testutil.Equals(test, timestamps[i], t)
		testutil.Equals(test, values[i], v)
		i += 1
	}
	testutil.Equals(test, len(timestamps), i)

	it = gmc.IteratorGroup(12000000, 0)
	testutil.Equals(test, false, it.Next())
}

func TestGMC2TruncateBefore(test *testing.T) {
	var timestamps []int64
	var values []float64
	for i := 0; i < 10000; i++ {
		timestamps = append(timestamps, int64(i*1000))
		values = append(values, rand.Float64())
	}

	gmc := &GroupMemoryChunk2{groupNum: 1, tupleSize: 4}
	app, _ := gmc.Appender()
	for i := 0; i < len(timestamps); i++ {
		app.AppendGroup(timestamps[i], []float64{values[i]})
	}

	{
		truncated, numDelete := gmc.TruncateBefore(3700*1000)
		testutil.Equals(test, 3700, numDelete)

		var t int64
		var v float64

		it := newGM2Iterator(truncated, 0, 0)
		i := 3700
		for it.Next() {
			t, v = it.At()
			testutil.Equals(test, timestamps[i], t)
			testutil.Equals(test, values[i], v)
			i += 1
		}
		testutil.Equals(test, len(timestamps), i)
	}

	{
		truncated, numDelete := gmc.TruncateBefore(7201*1000)
		testutil.Equals(test, 7201, numDelete)

		var t int64
		var v float64

		it := newGM2Iterator(truncated, 0, 0)
		i := 7201
		for it.Next() {
			t, v = it.At()
			testutil.Equals(test, timestamps[i], t)
			testutil.Equals(test, values[i], v)
			i += 1
		}
		testutil.Equals(test, len(timestamps), i)
	}

	{
		_, numDelete := gmc.TruncateBefore(10000*1000)
		testutil.Equals(test, 10000, numDelete)
	}

	{
		truncated, numDelete := gmc.TruncateBefore(9999*1000)
		testutil.Equals(test, 9999, numDelete)

		var t int64
		var v float64

		it := newGM2Iterator(truncated, 0, 0)
		i := 9999
		for it.Next() {
			t, v = it.At()
			testutil.Equals(test, timestamps[i], t)
			testutil.Equals(test, values[i], v)
			i += 1
		}
		testutil.Equals(test, len(timestamps), i)
	}

	{
		// Double truncated.
		truncated1, numDelete := gmc.TruncateBefore(3700*1000)
		testutil.Equals(test, 3700, numDelete)

		var t int64
		var v float64

		it := newGM2Iterator(truncated1, 0, 0)
		i := 3700
		for it.Next() {
			t, v = it.At()
			testutil.Equals(test, timestamps[i], t)
			testutil.Equals(test, values[i], v)
			i += 1
		}
		testutil.Equals(test, len(timestamps), i)

		truncated2, numDelete := truncated1.TruncateBefore(4300*1000)
		testutil.Equals(test, 600, numDelete)

		it = newGM2Iterator(truncated2, 0, 0)
		i = 4300
		for it.Next() {
			t, v = it.At()
			testutil.Equals(test, timestamps[i], t)
			testutil.Equals(test, values[i], v)
			i += 1
		}
		testutil.Equals(test, len(timestamps), i)
	}
}