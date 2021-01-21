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

func TestGMC1IncompleteTuple(test *testing.T) {
	timestamps := []int64{1092, 2344, 4444, 6666, 6667, 7777, 7778, 7779, 7780, 7781, 7782}
	values := []float64{0.3, 9, 241, 23.173, 1222.9874, 2131.22, 12.2, 22.2, 33.5, 43.8, 88.9}

	gmc := NewGroupMemoryChunk1(1)
	app, _ := gmc.Appender()
	for i := 0; i < len(timestamps); i++ {
		app.AppendGroup(timestamps[i], []float64{values[i]})
	}

	// gmc.Clean()

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

func TestGMC1OneSeries(test *testing.T) {
	timestamps := []int64{1092, 2344, 4444, 6666, 6667, 7777, 7778, 7779, 7780, 7781, 7782}
	values := []float64{0.3, 9, 241, 23.173, 1222.9874, 2131.22, 12.2, 22.2, 33.5, 43.8, 88.9}

	gmc := NewGroupMemoryChunk1(1)
	app, _ := gmc.Appender()
	for i := 0; i < len(timestamps); i++ {
		app.AppendGroup(timestamps[i], []float64{values[i]})
	}

	// gmc.Clean()

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

func TestGMC1TwoSeries(test *testing.T) {
	timestamps := []int64{1092, 2344, 4444, 6666, 6667, 7777, 7778, 7779, 7780, 7781, 7782}
	values := [][]float64{
		[]float64{0.3, 9, 241, 23.173, 1222.9874, 2131.22, 12.2, 22.2, 33.5, 43.8, 88.9},
		[]float64{0.74, 212.66, 9863.2, 443.019, 23, 654.999, 12.2, 22.2, 33.5, 43.8, 88.9},
	}

	gmc := &GroupMemoryChunk1{groupNum: 2, tupleSize: 4, startTime: math.MaxInt64}
	app, _ := gmc.Appender()
	for i := 0; i < len(timestamps); i++ {
		app.AppendGroup(timestamps[i], []float64{values[0][i], values[1][i]})
	}

	// gmc.Clean()

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

func TestGMC1TwoSeriesV2(test *testing.T) {
	gmc := NewGroupMemoryChunk1(2)
	app, _ := gmc.Appender()
	for i := 0; i < 4000; i += 5 {
		app.AppendGroup(int64(i), []float64{float64(i), float64(i)})
	}

	// gmc.Clean()

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

func TestGMC1ManyPoints(test *testing.T) {
	var timestamps []int64
	var values []float64
	for i := 0; i < 10000; i++ {
		timestamps = append(timestamps, int64(i))
		values = append(values, rand.Float64())
	}

	gmc := &GroupMemoryChunk1{groupNum: 1, tupleSize: 4, startTime: math.MaxInt64}
	app, _ := gmc.Appender()
	for i := 0; i < len(timestamps); i++ {
		app.AppendGroup(timestamps[i], []float64{values[i]})
	}

	// gmc.Clean()

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

	it = gmc.IteratorGroup(2000, 0)
	i = 2000
	for it.Next() {
		t, v = it.At()
		testutil.Equals(test, timestamps[i], t)
		testutil.Equals(test, values[i], v)
		i += 1
	}
	testutil.Equals(test, len(timestamps), i)

	it = gmc.IteratorGroup(5000, 0)
	i = 5000
	for it.Next() {
		t, v = it.At()
		testutil.Equals(test, timestamps[i], t)
		testutil.Equals(test, values[i], v)
		i += 1
	}
	testutil.Equals(test, len(timestamps), i)

	it = gmc.IteratorGroup(7777, 0)
	i = 7777
	for it.Next() {
		t, v = it.At()
		testutil.Equals(test, timestamps[i], t)
		testutil.Equals(test, values[i], v)
		i += 1
	}
	testutil.Equals(test, len(timestamps), i)

	it = gmc.IteratorGroup(12000, 0)
	testutil.Equals(test, false, it.Next())
}

func TestGMC1Size(t *testing.T) {
	numSeries := 10000
	groupSize := 100
	interval := 15
	devMag := 100
	scrapeCount := 3600 * 12 / interval
	{
		// Random data.
		randomGmcs := make([]*GroupMemoryChunk1, numSeries / groupSize)
		values := make([]float64, groupSize)
		for i := range randomGmcs {
			gmc := &GroupMemoryChunk1{groupNum: groupSize, tupleSize: 8, startTime: math.MaxInt64}
			app, _ := gmc.Appender()
			for j := 0; j < scrapeCount; j++ {
				for k := range values {
					values[k] = rand.Float64()
				}
				app.AppendGroup(int64(j * interval * 1000) + int64(float64(devMag) * rand.Float64()), values)
			}
			gmc.Clean()
			randomGmcs[i] = gmc
		}
		sizeOfValues := 0
		for _, gmc := range randomGmcs {
			for _, group := range gmc.group {
				sizeOfValues += len(group.b.stream)
			}
		}
		fmt.Println("random values size", sizeOfValues)
	}
	{
		// Counter data.
		counterGmcs := make([]*GroupMemoryChunk1, numSeries / groupSize)
		values := make([]float64, groupSize)
		for i := range counterGmcs {
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
			gmc.Clean()
			counterGmcs[i] = gmc
		}
		sizeOfValues := 0
		for _, gmc := range counterGmcs {
			for _, group := range gmc.group {
				sizeOfValues += len(group.b.stream)
			}
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
			file, err := os.Open("../testdata/bigdata/node_exporter/data" + strconv.Itoa(j))
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
			gmc.Clean()
			for _, group := range gmc.group {
				sizeOfValues += len(group.b.stream)
			}
		}
		fmt.Println("timeseries values size", sizeOfValues)
	}
}
