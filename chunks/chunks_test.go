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

package chunks

import (
	// "fmt"
	// "io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/naivewong/tsdb-group/chunkenc"
	"github.com/naivewong/tsdb-group/testutil"
)

func TestReaderWithInvalidBuffer(t *testing.T) {
	b := realByteSlice([]byte{0x81, 0x81, 0x81, 0x81, 0x81, 0x81})
	r := &Reader{bs: []ByteSlice{b}}

	_, err := r.Chunk(0)
	testutil.NotOk(t, err)
}

func TestGroupChunkRW(test *testing.T) {
	// tmpdir, err := ioutil.TempDir("", "test")
	// testutil.Ok(t, err)
	tmpdir := "testgcrw"
	defer func() {
		testutil.Ok(test, os.RemoveAll(tmpdir))
	}()

	numSeries := 20
	numPoints := 10000
	var timestamps []int64
	var data [][]float64

	gmc := chunkenc.NewGroupMemoryChunk1(numSeries)
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
		var chks []chunkenc.Meta
		for i := 0; i < numSeries; i++ {
			chks = append(chks, chunkenc.Meta{Chunk: gmc, MinTime: math.MinInt64})
		}

		var writer *Writer
		var reader *Reader
		var err    error
		var c      chunkenc.Chunk

		// Get the Writer and WriteChunks.
		writer, err = NewWriter(filepath.Join(tmpdir, "chunks1"))
		testutil.Ok(test, err)
		testutil.Ok(test, writer.WriteChunks(chks...))
		testutil.Ok(test, writer.Close())

		// Get Reader and get chunk.
		reader, err = NewDirReader(filepath.Join(tmpdir, "chunks1"), nil)
		testutil.Ok(test, err)
		c, err = reader.Chunk(chks[0].Ref)
		testutil.Ok(test, err)

		// Iterate from 0, series 0.
		it := c.IteratorGroup(0, int(chks[0].SeriesRef))
		testutil.NotEquals(test1, (*chunkenc.GD1Iterator)(nil), it)
		start := 0
		for it.Next() {
			t, v := it.At()
			testutil.Equals(test1, timestamps[start], t)
			testutil.Equals(test1, data[start][0], v)
			start += 1
		}
		testutil.Equals(test1, numPoints, start)

		// Iterate from 3, series 2.
		it = c.IteratorGroup(timestamps[3], int(chks[2].SeriesRef))
		testutil.NotEquals(test1, (*chunkenc.GD1Iterator)(nil), it)
		start = 3
		for it.Next() {
			t, v := it.At()
			testutil.Equals(test1, timestamps[start], t)
			testutil.Equals(test1, data[start][2], v)
			start += 1
		}
		testutil.Equals(test1, numPoints, start)

		// Iterate from 3, seek 5, series 10.
		it = c.IteratorGroup(timestamps[3], int(chks[10].SeriesRef))
		testutil.NotEquals(test1, (*chunkenc.GD1Iterator)(nil), it)
		testutil.Equals(test1, true, it.Seek(timestamps[5]))
		start = 5
		t, v := it.At()
		testutil.Equals(test1, timestamps[start], t)
		testutil.Equals(test1, data[start][10], v)
		start += 1
		for it.Next() {
			t, v := it.At()
			testutil.Equals(test1, timestamps[start], t)
			testutil.Equals(test1, data[start][10], v)
			start += 1
		}
		testutil.Equals(test1, numPoints, start)
	})
	test.Run("PartialConversion", func(test1 *testing.T) {
		var chks []chunkenc.Meta
		for i := 0; i < numSeries; i++ {
			chks = append(chks, chunkenc.Meta{Chunk: gmc, MinTime: timestamps[2], MaxTime: timestamps[numPoints-2]})
		}

		var writer *Writer
		var reader *Reader
		var err    error
		var c      chunkenc.Chunk

		// Get the Writer and WriteChunks.
		writer, err = NewWriter(filepath.Join(tmpdir, "chunks2"))
		testutil.Ok(test, err)
		testutil.Ok(test, writer.WriteChunks(chks...))
		testutil.Ok(test, writer.Close())

		// Get Reader and get chunk.
		reader, err = NewDirReader(filepath.Join(tmpdir, "chunks2"), nil)
		testutil.Ok(test, err)
		c, err = reader.Chunk(chks[0].Ref)
		testutil.Ok(test, err)

		// Iterate from 0, series 0.
		it := c.IteratorGroup(0, int(chks[0].SeriesRef))
		testutil.NotEquals(test1, (*chunkenc.GD1Iterator)(nil), it)
		start := 2
		for it.Next() {
			t, v := it.At()
			testutil.Equals(test1, timestamps[start], t)
			testutil.Equals(test1, data[start][0], v)
			start += 1
		}
		testutil.Equals(test1, numPoints-1, start)

		// Iterate from 3, series 2.
		it = c.IteratorGroup(timestamps[3], int(chks[2].SeriesRef))
		testutil.NotEquals(test1, (*chunkenc.GD1Iterator)(nil), it)
		start = 3
		for it.Next() {
			t, v := it.At()
			testutil.Equals(test1, timestamps[start], t)
			testutil.Equals(test1, data[start][2], v)
			start += 1
		}
		testutil.Equals(test1, numPoints-1, start)

		// Iterate from 3, seek 5, series 10.
		it = c.IteratorGroup(timestamps[3], int(chks[10].SeriesRef))
		testutil.NotEquals(test1, (*chunkenc.GD1Iterator)(nil), it)
		testutil.Equals(test1, true, it.Seek(timestamps[5]))
		start = 5
		t, v := it.At()
		testutil.Equals(test1, timestamps[start], t)
		testutil.Equals(test1, data[start][10], v)
		start += 1
		for it.Next() {
			t, v := it.At()
			testutil.Equals(test1, timestamps[start], t)
			testutil.Equals(test1, data[start][10], v)
			start += 1
		}
		testutil.Equals(test1, numPoints-1, start)
	})
}
