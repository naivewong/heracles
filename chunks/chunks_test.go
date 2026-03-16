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
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/naivewong/tsdb-group/chunkenc"
	"github.com/naivewong/tsdb-group/testutil"
	"github.com/pkg/errors"
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
		defer reader.Close()
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
		defer reader.Close()
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

func TestMergeChunks(t *testing.T) {
	// Create two XOR chunks with overlapping timestamps
	chunk1 := chunkenc.NewXORChunk()
	app1, err := chunk1.Appender()
	testutil.Ok(t, err)

	// Add samples: 0, 1, 2
	app1.Append(0, 0.0)
	app1.Append(1000, 1.0)
	app1.Append(2000, 2.0)

	chunk2 := chunkenc.NewXORChunk()
	app2, err := chunk2.Appender()
	testutil.Ok(t, err)

	// Add samples: 2, 3, 4 (overlapping at timestamp 2000)
	app2.Append(2000, 20.0) // Different value at same timestamp
	app2.Append(3000, 3.0)
	app2.Append(4000, 4.0)

	merged, err := MergeChunks(chunk1, chunk2)
	testutil.Ok(t, err)

	// Verify merged chunk contains all samples
	it := merged.Iterator()
	var count int
	expectedTimes := []int64{0, 1000, 2000, 3000, 4000}
	expectedValues := []float64{0.0, 1.0, 20.0, 3.0, 4.0} // Value at 2000 should be from chunk2

	for it.Next() {
		ts, v := it.At()
		testutil.Equals(t, expectedTimes[count], ts)
		testutil.Equals(t, expectedValues[count], v)
		count++
	}
	testutil.Equals(t, 5, count)
	testutil.Ok(t, it.Err())
}

func TestMergeChunksNoOverlap(t *testing.T) {
	// Create two XOR chunks without overlapping timestamps
	chunk1 := chunkenc.NewXORChunk()
	app1, err := chunk1.Appender()
	testutil.Ok(t, err)
	app1.Append(0, 0.0)
	app1.Append(1000, 1.0)

	chunk2 := chunkenc.NewXORChunk()
	app2, err := chunk2.Appender()
	testutil.Ok(t, err)
	app2.Append(2000, 2.0)
	app2.Append(3000, 3.0)

	merged, err := MergeChunks(chunk1, chunk2)
	testutil.Ok(t, err)

	it := merged.Iterator()
	var count int
	for it.Next() {
		count++
	}
	testutil.Equals(t, 4, count)
	testutil.Ok(t, it.Err())
}

func TestMergeOverlappingChunks(t *testing.T) {
	// Create test chunks
	chunk1 := chunkenc.NewXORChunk()
	app1, _ := chunk1.Appender()
	app1.Append(0, 0.0)
	app1.Append(1000, 1.0)

	chunk2 := chunkenc.NewXORChunk()
	app2, _ := chunk2.Appender()
	app2.Append(500, 0.5)  // Overlapping with chunk1
	app2.Append(1500, 1.5)

	chunk3 := chunkenc.NewXORChunk()
	app3, _ := chunk3.Appender()
	app3.Append(3000, 3.0) // Non-overlapping

	// Create meta with MinTime/MaxTime
	chks := []chunkenc.Meta{
		{Chunk: chunk1, MinTime: 0, MaxTime: 1000},
		{Chunk: chunk2, MinTime: 500, MaxTime: 1500},
		{Chunk: chunk3, MinTime: 3000, MaxTime: 3000},
	}

	merged, err := MergeOverlappingChunks(chks)
	testutil.Ok(t, err)

	// First two chunks should be merged, third remains separate
	testutil.Equals(t, 2, len(merged))
	testutil.Equals(t, int64(0), merged[0].MinTime)
	testutil.Equals(t, int64(1500), merged[0].MaxTime)
	testutil.Equals(t, int64(3000), merged[1].MinTime)
	testutil.Equals(t, int64(3000), merged[1].MaxTime)
}

func TestMergeOverlappingChunksSingleChunk(t *testing.T) {
	// Single chunk should return as-is
	chunk1 := chunkenc.NewXORChunk()
	app1, _ := chunk1.Appender()
	app1.Append(0, 0.0)

	chks := []chunkenc.Meta{
		{Chunk: chunk1, MinTime: 0, MaxTime: 0},
	}

	merged, err := MergeOverlappingChunks(chks)
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(merged))
}

func TestMergeOverlappingChunksNoOverlap(t *testing.T) {
	// Non-overlapping chunks should remain separate
	chunk1 := chunkenc.NewXORChunk()
	app1, _ := chunk1.Appender()
	app1.Append(0, 0.0)

	chunk2 := chunkenc.NewXORChunk()
	app2, _ := chunk2.Appender()
	app2.Append(2000, 2.0)

	chks := []chunkenc.Meta{
		{Chunk: chunk1, MinTime: 0, MaxTime: 0},
		{Chunk: chunk2, MinTime: 2000, MaxTime: 2000},
	}

	merged, err := MergeOverlappingChunks(chks)
	testutil.Ok(t, err)
	testutil.Equals(t, 2, len(merged))
}

func TestReaderSize(t *testing.T) {
	tmpdir := "testsize"
	defer os.RemoveAll(tmpdir)

	// Create a simple chunk
	gmc := chunkenc.NewGroupMemoryChunk1(1)
	app, _ := gmc.Appender()
	app.AppendGroup(0, []float64{1.0})
	app.AppendGroup(1000, []float64{2.0})

	chks := []chunkenc.Meta{{Chunk: gmc, MinTime: 0}}

	writer, err := NewWriter(filepath.Join(tmpdir, "chunks"))
	testutil.Ok(t, err)
	testutil.Ok(t, writer.WriteChunks(chks...))
	testutil.Ok(t, writer.Close())

	reader, err := NewDirReader(filepath.Join(tmpdir, "chunks"), nil)
	testutil.Ok(t, err)
	defer reader.Close()

	size := reader.Size()
	testutil.Assert(t, size > 0, "size should be greater than 0")
}

func TestByteSliceSub(t *testing.T) {
	b := realByteSlice([]byte{0x01, 0x02, 0x03, 0x04, 0x05})
	sub := b.Sub(1, 4)
	testutil.Equals(t, 3, sub.Len())
	testutil.Equals(t, []byte{0x02, 0x03, 0x04}, sub.Range(0, 3))
}

func TestSequenceFiles(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test_seq_files")
	testutil.Ok(t, err)
	defer os.RemoveAll(tmpdir)

	// Create some sequence files
	testutil.Ok(t, ioutil.WriteFile(filepath.Join(tmpdir, "000001"), []byte("data"), 0644))
	testutil.Ok(t, ioutil.WriteFile(filepath.Join(tmpdir, "000002"), []byte("data"), 0644))
	testutil.Ok(t, ioutil.WriteFile(filepath.Join(tmpdir, "000003"), []byte("data"), 0644))
	// Create a non-sequence file (should be ignored)
	testutil.Ok(t, ioutil.WriteFile(filepath.Join(tmpdir, "readme.txt"), []byte("readme"), 0644))

	files, err := sequenceFiles(tmpdir)
	testutil.Ok(t, err)
	testutil.Equals(t, 3, len(files))
}

func TestSequenceFilesEmpty(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test_seq_files_empty")
	testutil.Ok(t, err)
	defer os.RemoveAll(tmpdir)

	files, err := sequenceFiles(tmpdir)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(files))
}

func TestSequenceFilesNonExistent(t *testing.T) {
	_, err := sequenceFiles(filepath.Join(os.TempDir(), "nonexistent_dir_12345"))
	testutil.NotOk(t, err)
}

func TestNextSequenceFile(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test_next_seq")
	testutil.Ok(t, err)
	defer os.RemoveAll(tmpdir)

	// No existing files
	path, seq, err := nextSequenceFile(tmpdir)
	testutil.Ok(t, err)
	testutil.Equals(t, 1, seq)
	testutil.Equals(t, filepath.Join(tmpdir, "000001"), path)

	// Create first file
	testutil.Ok(t, ioutil.WriteFile(filepath.Join(tmpdir, "000001"), []byte("data"), 0644))

	path, seq, err = nextSequenceFile(tmpdir)
	testutil.Ok(t, err)
	testutil.Equals(t, 2, seq)
	testutil.Equals(t, filepath.Join(tmpdir, "000002"), path)

	// Create files with gaps
	testutil.Ok(t, ioutil.WriteFile(filepath.Join(tmpdir, "000005"), []byte("data"), 0644))

	_, seq, err = nextSequenceFile(tmpdir)
	testutil.Ok(t, err)
	testutil.Equals(t, 6, seq)
}

func TestNewDirReaderNonExistent(t *testing.T) {
	_, err := NewDirReader(filepath.Join(os.TempDir(), "nonexistent_dir_12345"), nil)
	testutil.NotOk(t, err)
}

func TestNewDirReaderEmpty(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test_empty_reader")
	testutil.Ok(t, err)
	defer os.RemoveAll(tmpdir)

	reader, err := NewDirReader(tmpdir, nil)
	testutil.Ok(t, err)
	// Empty directory returns a valid reader with size 0
	if reader != nil {
		testutil.Equals(t, int64(0), reader.Size())
		reader.Close()
	}
}

func TestWriterClose(t *testing.T) {
	tmpdir := "testwriterclose"
	defer os.RemoveAll(tmpdir)

	writer, err := NewWriter(filepath.Join(tmpdir, "chunks"))
	testutil.Ok(t, err)

	// Close without writing anything
	testutil.Ok(t, writer.Close())
}

func TestCloseAll(t *testing.T) {
	// Test with nil/empty slice
	err := closeAll(nil)
	testutil.Ok(t, err)

	// Test with multiple closers
	var closers []io.Closer
	closers = append(closers, &testCloser{})
	closers = append(closers, &testCloser{})

	err = closeAll(closers)
	testutil.Ok(t, err)
}

func TestCloseAllWithError(t *testing.T) {
	var closers []io.Closer
	closers = append(closers, &testCloser{})
	closers = append(closers, &errorCloser{})
	closers = append(closers, &testCloser{})

	err := closeAll(closers)
	testutil.NotOk(t, err)
}

// testCloser is a simple io.Closer implementation for testing
type testCloser struct{}

func (t *testCloser) Close() error { return nil }

// errorCloser is an io.Closer that always returns an error
type errorCloser struct{}

func (e *errorCloser) Close() error { return errors.New("close error") }

func TestChunkOutOfRange(t *testing.T) {
	tmpdir := "testchunkrange"
	defer os.RemoveAll(tmpdir)

	gmc := chunkenc.NewGroupMemoryChunk1(1)
	app, _ := gmc.Appender()
	app.AppendGroup(0, []float64{1.0})

	chks := []chunkenc.Meta{{Chunk: gmc, MinTime: 0}}

	writer, err := NewWriter(filepath.Join(tmpdir, "chunks"))
	testutil.Ok(t, err)
	testutil.Ok(t, writer.WriteChunks(chks...))
	testutil.Ok(t, writer.Close())

	reader, err := NewDirReader(filepath.Join(tmpdir, "chunks"), nil)
	testutil.Ok(t, err)
	defer reader.Close()

	// Try to get chunk with out-of-range sequence
	_, err = reader.Chunk(999 << 32)
	testutil.NotOk(t, err)
}
