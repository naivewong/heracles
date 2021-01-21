package chunkenc

import (
	"bufio"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/naivewong/tsdb-group/testutil"
)

func TestGMC1ToGDC1SingleSample(test *testing.T) {
	gmc := NewGroupMemoryChunk1(2)
	app, _ := gmc.Appender()
	app.AppendGroup(10086, []float64{2.3, 33.4})

	gdc := NewGroupDiskChunk1()
	var chks []Meta
	for i := 0; i < 2; i++ {
		chks = append(chks, Meta{})
	}

	testutil.Ok(test, gdc.ConvertGMC1(gmc, chks, math.MinInt64, math.MaxInt64))
	{
		it := gdc.IteratorGroup(0, int(chks[0].SeriesRef))
		testutil.NotEquals(test, (*GD1Iterator)(nil), it)
		testutil.Equals(test, true, it.Next())
		t, v := it.At()
		testutil.Equals(test, int64(10086), t)
		testutil.Equals(test, 2.3, v)
		testutil.Equals(test, false, it.Next())
	}
	{
		it := gdc.IteratorGroup(10086, int(chks[1].SeriesRef))
		testutil.NotEquals(test, (*GD1Iterator)(nil), it)
		testutil.Equals(test, true, it.Next())
		t, v := it.At()
		testutil.Equals(test, int64(10086), t)
		testutil.Equals(test, 33.4, v)
		testutil.Equals(test, false, it.Next())
	}
	{
		it := gdc.IteratorGroup(23947, int(chks[0].SeriesRef))
		testutil.NotEquals(test, (*GD1Iterator)(nil), it)
		testutil.Equals(test, false, it.Next())
	}
}

func TestGMC1ToGDC1SeveralSamples(test *testing.T) {
	gmc := NewGroupMemoryChunk1(2)
	app, _ := gmc.Appender()
	app.AppendGroup(10086, []float64{2.3, 33.4})
	app.AppendGroup(20086, []float64{5.3, 323.4})
	app.AppendGroup(30086, []float64{3.3, 333.4})

	gdc := NewGroupDiskChunk1()
	var chks []Meta
	for i := 0; i < 2; i++ {
		chks = append(chks, Meta{})
	}

	testutil.Ok(test, gdc.ConvertGMC1(gmc, chks, math.MinInt64, math.MaxInt64))
	{
		it := gdc.IteratorGroup(0, int(chks[0].SeriesRef))
		testutil.NotEquals(test, (*GD1Iterator)(nil), it)
		testutil.Equals(test, true, it.Next())
		t, v := it.At()
		testutil.Equals(test, int64(10086), t)
		testutil.Equals(test, 2.3, v)
		testutil.Equals(test, true, it.Next())
		t, v = it.At()
		testutil.Equals(test, int64(20086), t)
		testutil.Equals(test, 5.3, v)
		testutil.Equals(test, true, it.Next())
		t, v = it.At()
		testutil.Equals(test, int64(30086), t)
		testutil.Equals(test, 3.3, v)
		testutil.Equals(test, false, it.Next())
	}
	{
		it := gdc.IteratorGroup(10086, int(chks[1].SeriesRef))
		testutil.NotEquals(test, (*GD1Iterator)(nil), it)
		testutil.Equals(test, true, it.Next())
		t, v := it.At()
		testutil.Equals(test, int64(10086), t)
		testutil.Equals(test, 33.4, v)
		testutil.Equals(test, true, it.Next())
		t, v = it.At()
		testutil.Equals(test, int64(20086), t)
		testutil.Equals(test, 323.4, v)
		testutil.Equals(test, true, it.Next())
		t, v = it.At()
		testutil.Equals(test, int64(30086), t)
		testutil.Equals(test, 333.4, v)
		testutil.Equals(test, false, it.Next())
	}
	{
		it := gdc.IteratorGroup(232947, int(chks[0].SeriesRef))
		testutil.NotEquals(test, (*GD1Iterator)(nil), it)
		testutil.Equals(test, false, it.Next())
	}
}

func TestGMC2ToGDC1SingleSample(test *testing.T) {
	gmc := NewGroupMemoryChunk2(2)
	app, _ := gmc.Appender()
	app.AppendGroup(10086, []float64{2.3, 33.4})

	gdc := NewGroupDiskChunk1()
	var chks []Meta
	for i := 0; i < 2; i++ {
		chks = append(chks, Meta{})
	}

	testutil.Ok(test, gdc.ConvertGMC2(gmc, chks, math.MinInt64, math.MaxInt64))
	{
		it := gdc.IteratorGroup(0, int(chks[0].SeriesRef))
		testutil.NotEquals(test, (*GD1Iterator)(nil), it)
		testutil.Equals(test, true, it.Next())
		t, v := it.At()
		testutil.Equals(test, int64(10086), t)
		testutil.Equals(test, 2.3, v)
		testutil.Equals(test, false, it.Next())
	}
	{
		it := gdc.IteratorGroup(10086, int(chks[1].SeriesRef))
		testutil.NotEquals(test, (*GD1Iterator)(nil), it)
		testutil.Equals(test, true, it.Next())
		t, v := it.At()
		testutil.Equals(test, int64(10086), t)
		testutil.Equals(test, 33.4, v)
		testutil.Equals(test, false, it.Next())
	}
	{
		it := gdc.IteratorGroup(23947, int(chks[0].SeriesRef))
		testutil.NotEquals(test, (*GD1Iterator)(nil), it)
		testutil.Equals(test, false, it.Next())
	}
}

func TestGMC1ToGDC1(test *testing.T) {
	numSeries := 100
	numPoints := 10000
	var timestamps []int64
	var data [][]float64

	gmc := NewGroupMemoryChunk1(numSeries)
	app, _ := gmc.Appender()
	for i := 0; i < numPoints; i++ {
		timestamps = append(timestamps, int64(1000*i) + rand.Int63n(100))
		var values []float64
		for j := 0; j < numSeries; j++ {
			values = append(values, rand.Float64())
			// values = append(values, float64(i))
		}
		data = append(data, values)
		app.AppendGroup(timestamps[len(timestamps)-1], values)
	}

	test.Run("WholeRangeConversion", func(test1 *testing.T) {
		gdc := NewGroupDiskChunk1()
		var chks []Meta
		for i := 0; i < numSeries; i++ {
			chks = append(chks, Meta{})
		}

		testutil.Ok(test1, gdc.ConvertGMC1(gmc, chks, math.MinInt64, math.MaxInt64))

		{
			it := newGD1Iterator(gdc, 0, int(chks[0].SeriesRef))
			testutil.NotEquals(test1, (*GD1Iterator)(nil), it)
			start := 0
			for it.Next() {
				t, v := it.At()
				testutil.Equals(test1, timestamps[start], t)
				testutil.Equals(test1, data[start][0], v)
				start += 1
			}
			testutil.Equals(test1, numPoints, start)
		}
		{
			// Test Seek().
			it := newGD1Iterator(gdc, 0, int(chks[0].SeriesRef))
			testutil.NotEquals(test1, (*GD1Iterator)(nil), it)
			testutil.Equals(test1, true, it.Seek(timestamps[3500]))
			t, v := it.At()
			testutil.Equals(test1, timestamps[3500], t)
			testutil.Equals(test1, data[3500][0], v)
			start := 3501
			for it.Next() {
				t, v = it.At()
				testutil.Equals(test1, timestamps[start], t)
				testutil.Equals(test1, data[start][0], v)
				start += 1
			}
			testutil.Equals(test1, numPoints, start)
		}
		{
			// Test Seek().
			it := newGD1Iterator(gdc, 0, int(chks[0].SeriesRef))
			testutil.NotEquals(test1, (*GD1Iterator)(nil), it)
			testutil.Equals(test1, true, it.Seek(timestamps[7000]))
			t, v := it.At()
			testutil.Equals(test1, timestamps[7000], t)
			testutil.Equals(test1, data[7000][0], v)
			start := 7001
			for it.Next() {
				t, v = it.At()
				testutil.Equals(test1, timestamps[start], t)
				testutil.Equals(test1, data[start][0], v)
				start += 1
			}
			testutil.Equals(test1, numPoints, start)
		}
		{
			it := gdc.IteratorGroup(timestamps[3], int(chks[0].SeriesRef))
			testutil.NotEquals(test1, (*GD1Iterator)(nil), it)
			start := 3
			for it.Next() {
				t, v := it.At()
				testutil.Equals(test1, timestamps[start], t)
				testutil.Equals(test1, data[start][0], v)
				start += 1
			}
			testutil.Equals(test1, numPoints, start)
		}
		{
			it := gdc.IteratorGroup(timestamps[3], int(chks[0].SeriesRef))
			testutil.NotEquals(test1, (*GD1Iterator)(nil), it)
			testutil.Equals(test1, true, it.Seek(timestamps[5]))
			start := 5
			t, v := it.At()
			testutil.Equals(test1, timestamps[start], t)
			testutil.Equals(test1, data[start][0], v)
			start += 1
			for it.Next() {
				t, v := it.At()
				testutil.Equals(test1, timestamps[start], t)
				testutil.Equals(test1, data[start][0], v)
				start += 1
			}
			testutil.Equals(test1, numPoints, start)
		}
	})

	test.Run("PartialRangeConversion", func(test1 *testing.T) {
		gdc := NewGroupDiskChunk1()
		var chks []Meta
		for i := 0; i < numSeries; i++ {
			chks = append(chks, Meta{})
		}

		testutil.Ok(test1, gdc.ConvertGMC1(gmc, chks, timestamps[2], timestamps[numPoints-2]))

		it := gdc.IteratorGroup(0, int(chks[0].SeriesRef))
		testutil.NotEquals(test1, (*GD1Iterator)(nil), it)
		start := 2
		for it.Next() {
			t, v := it.At()
			testutil.Equals(test1, timestamps[start], t)
			testutil.Equals(test1, data[start][0], v)
			start += 1
		}
		testutil.Equals(test1, numPoints-1, start)

		it = gdc.IteratorGroup(timestamps[3], int(chks[0].SeriesRef))
		testutil.NotEquals(test1, (*GD1Iterator)(nil), it)
		start = 3
		for it.Next() {
			t, v := it.At()
			testutil.Equals(test1, timestamps[start], t)
			testutil.Equals(test1, data[start][0], v)
			start += 1
		}
		testutil.Equals(test1, numPoints-1, start)

		it = gdc.IteratorGroup(timestamps[3], int(chks[0].SeriesRef))
		testutil.NotEquals(test1, (*GD1Iterator)(nil), it)
		testutil.Equals(test1, true, it.Seek(timestamps[5]))
		start = 5
		t, v := it.At()
		testutil.Equals(test1, timestamps[start], t)
		testutil.Equals(test1, data[start][0], v)
		start += 1
		for it.Next() {
			t, v := it.At()
			testutil.Equals(test1, timestamps[start], t)
			testutil.Equals(test1, data[start][0], v)
			start += 1
		}
		testutil.Equals(test1, numPoints-1, start)
	})
}

func TestGMC2ToGDC1(test *testing.T) {
	numSeries := 100
	numPoints := 10000
	var timestamps []int64
	var data [][]float64

	gmc := NewGroupMemoryChunk2(numSeries)
	app, _ := gmc.Appender()
	for i := 0; i < numPoints; i++ {
		timestamps = append(timestamps, int64(1000*i) + rand.Int63n(100))
		var values []float64
		for j := 0; j < numSeries; j++ {
			values = append(values, rand.Float64())
			// values = append(values, float64(i))
		}
		data = append(data, values)
		app.AppendGroup(timestamps[len(timestamps)-1], values)
	}

	test.Run("WholeRangeConversion", func(test1 *testing.T) {
		gdc := NewGroupDiskChunk1()
		var chks []Meta
		for i := 0; i < numSeries; i++ {
			chks = append(chks, Meta{})
		}

		testutil.Ok(test1, gdc.ConvertGMC2(gmc, chks, math.MinInt64, math.MaxInt64))

		it := gdc.IteratorGroup(0, int(chks[0].SeriesRef))
		testutil.NotEquals(test1, (*GD1Iterator)(nil), it)
		start := 0
		for it.Next() {
			t, v := it.At()
			testutil.Equals(test1, timestamps[start], t)
			testutil.Equals(test1, data[start][0], v)
			start += 1
		}
		testutil.Equals(test1, numPoints, start)

		it = gdc.IteratorGroup(timestamps[3], int(chks[0].SeriesRef))
		testutil.NotEquals(test1, (*GD1Iterator)(nil), it)
		start = 3
		for it.Next() {
			t, v := it.At()
			testutil.Equals(test1, timestamps[start], t)
			testutil.Equals(test1, data[start][0], v)
			start += 1
		}
		testutil.Equals(test1, numPoints, start)

		it = gdc.IteratorGroup(timestamps[3], int(chks[0].SeriesRef))
		testutil.NotEquals(test1, (*GD1Iterator)(nil), it)
		testutil.Equals(test1, true, it.Seek(timestamps[5]))
		start = 5
		t, v := it.At()
		testutil.Equals(test1, timestamps[start], t)
		testutil.Equals(test1, data[start][0], v)
		start += 1
		for it.Next() {
			t, v := it.At()
			testutil.Equals(test1, timestamps[start], t)
			testutil.Equals(test1, data[start][0], v)
			start += 1
		}
		testutil.Equals(test1, numPoints, start)
	})

	test.Run("PartialRangeConversion1", func(test1 *testing.T) {
		gdc := NewGroupDiskChunk1()
		var chks []Meta
		for i := 0; i < numSeries; i++ {
			chks = append(chks, Meta{})
		}

		testutil.Ok(test1, gdc.ConvertGMC2(gmc, chks, timestamps[2], timestamps[numPoints-2]))

		it := gdc.IteratorGroup(0, int(chks[0].SeriesRef))
		testutil.NotEquals(test1, (*GD1Iterator)(nil), it)
		start := 2
		for it.Next() {
			t, v := it.At()
			testutil.Equals(test1, timestamps[start], t)
			testutil.Equals(test1, data[start][0], v)
			start += 1
		}
		testutil.Equals(test1, numPoints-1, start)

		it = gdc.IteratorGroup(timestamps[3], int(chks[0].SeriesRef))
		testutil.NotEquals(test1, (*GD1Iterator)(nil), it)
		start = 3
		for it.Next() {
			t, v := it.At()
			testutil.Equals(test1, timestamps[start], t)
			testutil.Equals(test1, data[start][0], v)
			start += 1
		}
		testutil.Equals(test1, numPoints-1, start)

		it = gdc.IteratorGroup(timestamps[3], int(chks[0].SeriesRef))
		testutil.NotEquals(test1, (*GD1Iterator)(nil), it)
		testutil.Equals(test1, true, it.Seek(timestamps[5]))
		start = 5
		t, v := it.At()
		testutil.Equals(test1, timestamps[start], t)
		testutil.Equals(test1, data[start][0], v)
		start += 1
		for it.Next() {
			t, v := it.At()
			testutil.Equals(test1, timestamps[start], t)
			testutil.Equals(test1, data[start][0], v)
			start += 1
		}
		testutil.Equals(test1, numPoints-1, start)
	})

	test.Run("PartialRangeConversion2", func(test1 *testing.T) {
		gdc := NewGroupDiskChunk1()
		var chks []Meta
		for i := 0; i < numSeries; i++ {
			chks = append(chks, Meta{})
		}

		testutil.Ok(test1, gdc.ConvertGMC2(gmc, chks, timestamps[2], timestamps[numPoints/3]-int64(1)))

		it := gdc.IteratorGroup(0, int(chks[0].SeriesRef))
		testutil.NotEquals(test1, (*GD1Iterator)(nil), it)
		start := 2
		for it.Next() {
			t, v := it.At()
			testutil.Equals(test1, timestamps[start], t)
			testutil.Equals(test1, data[start][0], v)
			start += 1
		}
		testutil.Equals(test1, numPoints/3, start)

		it = gdc.IteratorGroup(timestamps[3], int(chks[0].SeriesRef))
		testutil.NotEquals(test1, (*GD1Iterator)(nil), it)
		start = 3
		for it.Next() {
			t, v := it.At()
			testutil.Equals(test1, timestamps[start], t)
			testutil.Equals(test1, data[start][0], v)
			start += 1
		}
		testutil.Equals(test1, numPoints/3, start)

		it = gdc.IteratorGroup(timestamps[3], int(chks[0].SeriesRef))
		testutil.NotEquals(test1, (*GD1Iterator)(nil), it)
		testutil.Equals(test1, true, it.Seek(timestamps[5]))
		start = 5
		t, v := it.At()
		testutil.Equals(test1, timestamps[start], t)
		testutil.Equals(test1, data[start][0], v)
		start += 1
		for it.Next() {
			t, v := it.At()
			testutil.Equals(test1, timestamps[start], t)
			testutil.Equals(test1, data[start][0], v)
			start += 1
		}
		testutil.Equals(test1, numPoints/3, start)
	})
}

func TestGDC1Size(t *testing.T) {
	numSeries := 10000
	groupSize := 100
	interval := 15
	devMag := 100
	scrapeCount := 3600 * 12 / interval
	{
		// Random data.
		randomGdcs := make([]*GroupDiskChunk1, numSeries / groupSize)
		values := make([]float64, groupSize)
		for i := range randomGdcs {
			gmc := &GroupMemoryChunk1{groupNum: groupSize, tupleSize: 8, startTime: math.MaxInt64}
			app, _ := gmc.Appender()
			for j := 0; j < scrapeCount; j++ {
				for k := range values {
					values[k] = rand.Float64()
				}
				app.AppendGroup(int64(j * interval * 1000) + int64(float64(devMag) * rand.Float64()), values)
			}
			gdc := NewGroupDiskChunk1()
			var chks []Meta
			for i := 0; i < numSeries; i++ {
				chks = append(chks, Meta{})
			}
			testutil.Ok(t, gdc.ConvertGMC1(gmc, chks, math.MinInt64, math.MaxInt64))
			randomGdcs[i] = gdc
		}
		sizeOfValues := 0
		for _, gdc := range randomGdcs {
			sizeOfValues += gdc.Size()
		}
		fmt.Println("random values size", sizeOfValues)
	}
	{
		// Counter data.
		counterGdcs := make([]*GroupDiskChunk1, numSeries / groupSize)
		values := make([]float64, groupSize)
		for i := range counterGdcs {
			gmc := &GroupMemoryChunk1{groupNum: groupSize, tupleSize: 8, startTime: math.MaxInt64}
			app, _ := gmc.Appender()
			counter := int64(123456789)
			for j := 0; j < scrapeCount; j++ {
				for k := range values {
					values[k] = float64(counter)
				}
				counter += int64(1000)
				app.AppendGroup(int64(j * interval * 1000) + int64(float64(devMag) * rand.Float64()), values)
			}
			gdc := NewGroupDiskChunk1()
			var chks []Meta
			for i := 0; i < numSeries; i++ {
				chks = append(chks, Meta{})
			}
			testutil.Ok(t, gdc.ConvertGMC1(gmc, chks, math.MinInt64, math.MaxInt64))
			counterGdcs[i] = gdc
		}
		sizeOfValues := 0
		for _, gdc := range counterGdcs {
			sizeOfValues += gdc.Size()
		}
		fmt.Println("counter values size", sizeOfValues)
	}
	{
		// Timeseries data.
		timeseriesGmcs := make([]*GroupMemoryChunk1, numSeries / groupSize)
		for i := range timeseriesGmcs {
			timeseriesGmcs[i] = &GroupMemoryChunk1{groupNum: groupSize, tupleSize: 8, startTime: math.MaxInt64}
		}
		scanners := make([]*bufio.Scanner, numSeries/500)
		files := make([]*os.File, numSeries/500)
		for j := 0; j < numSeries/500; j++ {
			file, err := os.Open("../testdata/bigdata/node_exporter/data" + strconv.Itoa(j+20))
			testutil.Ok(t, err)
			scanners[j] = bufio.NewScanner(file)
			files[j] = file
		}

		k := 0
		for scanners[0].Scan() {
			gid := 0
			idx := 0
			t_ := int64(k * interval * 1000) + int64(rand.Float64()*float64(devMag))
			{
				s := scanners[0].Text()
				data := strings.Split(s, ",")
				for i := idx; i < idx+500; i += groupSize {
					tempData := make([]float64, 0, groupSize)
					for j := i; j < i + groupSize; j++ {
						v, _ := strconv.ParseFloat(data[j-idx], 64)
						tempData = append(tempData, v)
					}
					app, _ := timeseriesGmcs[gid].Appender()
					app.AppendGroup(t_, tempData)
					gid++
				}

				for i := 0; i < interval/5000-1; i++ {
					scanners[0].Scan()
				}
				idx += 500
			}
			for sc := 1; sc < numSeries/500; sc++ {
				if idx >= numSeries {
					break
				}
				end := idx + 500
				if end > numSeries {
					end = numSeries
				}
				scanners[sc].Scan()
				s := scanners[sc].Text()
				data := strings.Split(s, ",")
				for i := idx; i < end; i += groupSize {
					tempData := make([]float64, 0, groupSize)
					for j := i; j < i + groupSize; j++ {
						v, _ := strconv.ParseFloat(data[j-idx], 64)
						tempData = append(tempData, v)
					}
					app, _ := timeseriesGmcs[gid].Appender()
					app.AppendGroup(t_, tempData)
					gid++
				}

				for i := 0; i < interval/5000-1; i++ {
					scanners[sc].Scan()
				}
				idx = end
			}
			k += 1
		}

		for _, f := range files {
			f.Close()
		}

		sizeOfValues := 0
		for _, gmc := range timeseriesGmcs {
			gdc := NewGroupDiskChunk1()
			var chks []Meta
			for i := 0; i < numSeries; i++ {
				chks = append(chks, Meta{})
			}
			testutil.Ok(t, gdc.ConvertGMC1(gmc, chks, math.MinInt64, math.MaxInt64))
			sizeOfValues += gdc.Size()
		}
		fmt.Println("timeseries values size", sizeOfValues)
	}
}
