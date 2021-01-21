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

package tsdb

import (
	"context"
	"encoding/binary"
	"errors"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/naivewong/tsdb-group/chunks"
	"github.com/naivewong/tsdb-group/index"
	"github.com/naivewong/tsdb-group/labels"
	"github.com/naivewong/tsdb-group/testutil"
	"github.com/naivewong/tsdb-group/tsdbutil"
)

// In Prometheus 2.1.0 we had a bug where the meta.json version was falsely bumped
// to 2. We had a migration in place resetting it to 1 but we should move immediately to
// version 3 next time to avoid confusion and issues.
func TestBlockMetaMustNeverBeVersion2(t *testing.T) {
	dir, err := ioutil.TempDir("", "metaversion")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(dir))
	}()

	_, err = writeMetaFile(log.NewNopLogger(), dir, &BlockMeta{})
	testutil.Ok(t, err)

	meta, _, err := readMetaFile(dir)
	testutil.Ok(t, err)
	testutil.Assert(t, meta.Version != 2, "meta.json version must never be 2")
}

func TestSetCompactionFailed(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(tmpdir))
	}()

	blockDir := createBlock(t, tmpdir, genSeries(1, 1, 0, 0), 1)
	b, err := OpenBlock(nil, blockDir, nil)
	testutil.Ok(t, err)
	testutil.Equals(t, false, b.meta.Compaction.Failed)
	testutil.Ok(t, b.setCompactionFailed())
	testutil.Equals(t, true, b.meta.Compaction.Failed)
	testutil.Ok(t, b.Close())

	b, err = OpenBlock(nil, blockDir, nil)
	testutil.Ok(t, err)
	testutil.Equals(t, true, b.meta.Compaction.Failed)
	testutil.Ok(t, b.Close())
}

func TestCreateBlock(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(tmpdir))
	}()
	b, err := OpenBlock(nil, createBlock(t, tmpdir, genSeries(1, 1, 0, 10), 1), nil)
	if err == nil {
		testutil.Ok(t, b.Close())
	}
	testutil.Ok(t, err)
}

func TestCorruptedChunk(t *testing.T) {
	for name, test := range map[string]struct {
		corrFunc func(f *os.File) // Func that applies the corruption.
		expErr   error
	}{
		"invalid header size": {
			func(f *os.File) {
				err := f.Truncate(1)
				testutil.Ok(t, err)
			},
			errors.New("invalid chunk header in segment 0: invalid size"),
		},
		"invalid magic number": {
			func(f *os.File) {
				magicChunksOffset := int64(0)
				_, err := f.Seek(magicChunksOffset, 0)
				testutil.Ok(t, err)

				// Set invalid magic number.
				b := make([]byte, chunks.MagicChunksSize)
				binary.BigEndian.PutUint32(b[:chunks.MagicChunksSize], 0x00000000)
				n, err := f.Write(b)
				testutil.Ok(t, err)
				testutil.Equals(t, chunks.MagicChunksSize, n)
			},
			errors.New("invalid magic number 0"),
		},
		"invalid chunk format version": {
			func(f *os.File) {
				chunksFormatVersionOffset := int64(4)
				_, err := f.Seek(chunksFormatVersionOffset, 0)
				testutil.Ok(t, err)

				// Set invalid chunk format version.
				b := make([]byte, chunks.ChunksFormatVersionSize)
				b[0] = 0
				n, err := f.Write(b)
				testutil.Ok(t, err)
				testutil.Equals(t, chunks.ChunksFormatVersionSize, n)
			},
			errors.New("invalid chunk format version 0"),
		},
	} {
		t.Run(name, func(t *testing.T) {
			tmpdir, err := ioutil.TempDir("", "test_open_block_chunk_corrupted")
			testutil.Ok(t, err)
			defer func() {
				testutil.Ok(t, os.RemoveAll(tmpdir))
			}()

			blockDir := createBlock(t, tmpdir, genSeries(1, 1, 0, 0), 1)
			files, err := sequenceFiles(chunkDir(blockDir))
			testutil.Ok(t, err)
			testutil.Assert(t, len(files) > 0, "No chunk created.")

			f, err := os.OpenFile(files[0], os.O_RDWR, 0666)
			testutil.Ok(t, err)

			// Apply corruption function.
			test.corrFunc(f)
			testutil.Ok(t, f.Close())

			_, err = OpenBlock(nil, blockDir, nil)
			testutil.Equals(t, test.expErr.Error(), err.Error())
		})
	}
}

// TestBlockSize ensures that the block size is calculated correctly.
func TestBlockSize(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test_blockSize")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(tmpdir))
	}()

	var (
		blockInit    *Block
		expSizeInit  int64
		blockDirInit string
	)

	// Create a block and compare the reported size vs actual disk size.
	{
		blockDirInit = createBlock(t, tmpdir, genSeries(10, 1, 1, 100), 1)
		blockInit, err = OpenBlock(nil, blockDirInit, nil)
		testutil.Ok(t, err)
		defer func() {
			testutil.Ok(t, blockInit.Close())
		}()
		expSizeInit = blockInit.Size()
		actSizeInit, err := testutil.DirSize(blockInit.Dir())
		testutil.Ok(t, err)
		testutil.Equals(t, expSizeInit, actSizeInit)
	}

	// Delete some series and check the sizes again.
	{
		testutil.Ok(t, blockInit.Delete(1, 10, labels.NewMustRegexpMatcher("", ".*")))
		expAfterDelete := blockInit.Size()
		testutil.Assert(t, expAfterDelete > expSizeInit, "after a delete the block size should be bigger as the tombstone file should grow %v > %v", expAfterDelete, expSizeInit)
		actAfterDelete, err := testutil.DirSize(blockDirInit)
		testutil.Ok(t, err)
		testutil.Equals(t, expAfterDelete, actAfterDelete, "after a delete reported block size doesn't match actual disk size")

		c, err := NewLeveledCompactor(context.Background(), nil, log.NewNopLogger(), []int64{0}, nil)
		testutil.Ok(t, err)
		blockDirAfterCompact, err := c.Compact(tmpdir, []string{blockInit.Dir()}, nil)
		testutil.Ok(t, err)
		blockAfterCompact, err := OpenBlock(nil, filepath.Join(tmpdir, blockDirAfterCompact.String()), nil)
		testutil.Ok(t, err)
		defer func() {
			testutil.Ok(t, blockAfterCompact.Close())
		}()
		expAfterCompact := blockAfterCompact.Size()
		actAfterCompact, err := testutil.DirSize(blockAfterCompact.Dir())
		testutil.Ok(t, err)
		testutil.Assert(t, actAfterDelete > actAfterCompact, "after a delete and compaction the block size should be smaller %v,%v", actAfterDelete, actAfterCompact)
		testutil.Equals(t, expAfterCompact, actAfterCompact, "after a delete and compaction reported block size doesn't match actual disk size")
	}
}

func TestBlockAll(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(tmpdir))
	}()
	b, err := OpenBlock(nil, createBlock(t, tmpdir, genSeries(10, 2, 0, 100), 5), nil)
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, b.Close())
	}()

	{
		// Test index.
		ir, err := b.Index()
		testutil.Ok(t, err)

		// Test label names.
		lnames, err := ir.LabelNames()
		testutil.Ok(t, err)
		testutil.Equals(t, []string{"labelName", "labelName1"}, lnames)

		// Test label values.
		lvals, err := ir.LabelValues("labelName")
		testutil.Ok(t, err)
		testutil.Equals(t, 10, lvals.Len())
		for i := 0; i < lvals.Len(); i++ {
			v, _ := lvals.At(i)
			testutil.Equals(t, strconv.Itoa(i), v[0])
		}
		lvals, err = ir.LabelValues("labelName1")
		testutil.Ok(t, err)
		testutil.Equals(t, 1, lvals.Len())
		v, _ := lvals.At(0)
		testutil.Equals(t, "labelValue1", v[0])

		// Test group postings (index.AllGroupPostings).
		p, err := ir.GroupPostings(index.AllGroupPostings)
		testutil.Ok(t, err)
		arr, err := index.ExpandPostings(p)
		testutil.Ok(t, err)
		testutil.Equals(t, []uint64{0, 1}, arr)

		// Test group postings.
		p, err = ir.GroupPostings(0)
		testutil.Ok(t, err)
		arr, err = index.ExpandPostings(p)
		testutil.Ok(t, err)
		testutil.Equals(t, 5, len(arr))
		p, err = ir.GroupPostings(1)
		testutil.Ok(t, err)
		arr, err = index.ExpandPostings(p)
		testutil.Ok(t, err)
		testutil.Equals(t, 5, len(arr))

		// Test postings.
		p, err = ir.Postings("labelName", "0")
		testutil.Ok(t, err)
		arr, err = index.ExpandPostings(p)
		testutil.Ok(t, err)
		testutil.Equals(t, 1, len(arr))
		p, err = ir.Postings("labelName1", "labelValue1")
		testutil.Ok(t, err)
		arr, err = index.ExpandPostings(p)
		testutil.Ok(t, err)
		testutil.Equals(t, 10, len(arr))
		ir.Close()
	}
	{
		// Test del.
		b.Delete(10, 20, labels.NewEqualMatcher("labelName1", "labelValue1"))
		b1, err := OpenBlock(nil, b.Dir(), nil)
		testutil.Ok(t, err)
		defer func() {
			testutil.Ok(t, b1.Close())
		}()

		stones, err := b1.Tombstones()
		testutil.Ok(t, err)
		itvls, err := stones.Get(0)
		testutil.Ok(t, err)
		testutil.Equals(t, Intervals{{10, 20}}, itvls)
		itvls, err = stones.Get(1)
		testutil.Ok(t, err)
		testutil.Equals(t, Intervals{{10, 20}}, itvls)
		stones.Close()
	}
}

// createBlock creates a block with given set of series and returns its dir.
func createBlock(tb testing.TB, dir string, series []Series, groupSize int) string {
	head := createHead(tb, series, groupSize)
	compactor, err := NewLeveledCompactor(context.Background(), nil, log.NewNopLogger(), []int64{1000000}, nil)
	testutil.Ok(tb, err)

	testutil.Ok(tb, os.MkdirAll(dir, 0777))

	ulid, err := compactor.Write(dir, head, head.MinTime(), head.MaxTime(), nil)
	testutil.Ok(tb, err)
	return filepath.Join(dir, ulid.String())
}

func createHead(tb testing.TB, series []Series, groupSize int) *Head {
	head, err := NewHead(nil, nil, nil, 2*60*60*1000)
	testutil.Ok(tb, err)
	defer head.Close()

	app := head.Appender()
	for i := 0; i < len(series); i += groupSize {
		ref := uint64(0)
		its := []SeriesIterator{}
		l := i+groupSize
		if len(series) < l {
			l = len(series)
		}
		lsets := make([]labels.Labels, 0, len(its))
		for j := i; j < l; j++ {
			its = append(its, series[j].Iterator())
			lsets = append(lsets, series[j].Labels())
		}
		for its[0].Next() {
			t, v := its[0].At()
			vals := make([]float64, 0, len(its))
			vals = append(vals, v)
			for j := 1; j < len(its); j++ {
				its[j].Next()
				_, v = its[j].At()
				vals = append(vals, v)
			}
			if ref != 0 {
				err := app.AddGroupFast(ref, t, vals)
				if err == nil {
					continue
				}
			}
			ref, err = app.AddGroup(lsets, t, vals)
			testutil.Ok(tb, err)
		}
		testutil.Ok(tb, its[0].Err())
	}
	err = app.Commit()
	testutil.Ok(tb, err)
	return head
}

const (
	defaultLabelName  = "labelName"
	defaultLabelValue = "labelValue"
)

// genSeries generates series with a given number of labels and values.
func genSeries(totalSeries, labelCount int, mint, maxt int64) []Series {
	if totalSeries == 0 || labelCount == 0 {
		return nil
	}

	series := make([]Series, totalSeries)

	for i := 0; i < totalSeries; i++ {
		lbls := make(map[string]string, labelCount)
		lbls[defaultLabelName] = strconv.Itoa(i)
		for j := 1; len(lbls) < labelCount; j++ {
			lbls[defaultLabelName+strconv.Itoa(j)] = defaultLabelValue + strconv.Itoa(j)
		}
		samples := make([]tsdbutil.Sample, 0, maxt-mint+1)
		for t := mint; t < maxt; t++ {
			samples = append(samples, sample{t: t, v: rand.Float64()})
		}
		series[i] = newSeries(lbls, samples)
	}
	return series
}

func genSeriesOrdered(totalSeries, labelCount int, mint, maxt int64) []Series {
	if totalSeries == 0 || labelCount == 0 {
		return nil
	}

	series := make([]Series, totalSeries)

	for i := 0; i < totalSeries; i++ {
		lbls := make(map[string]string, labelCount)
		lbls[defaultLabelName] = strconv.Itoa(i)
		for j := 1; len(lbls) < labelCount; j++ {
			lbls[defaultLabelName+strconv.Itoa(j)] = defaultLabelValue + strconv.Itoa(j)
		}
		samples := make([]tsdbutil.Sample, 0, maxt-mint+1)
		for t := mint; t < maxt; t++ {
			samples = append(samples, sample{t: t, v: float64(t)})
		}
		series[i] = newSeries(lbls, samples)
	}
	return series
}
func genSeriesOrderedPlusOne(totalSeries, labelCount int, mint, maxt int64) []Series {
	if totalSeries == 0 || labelCount == 0 {
		return nil
	}

	series := make([]Series, totalSeries)

	for i := 0; i < totalSeries; i++ {
		lbls := make(map[string]string, labelCount)
		lbls[defaultLabelName] = strconv.Itoa(i)
		for j := 1; len(lbls) < labelCount; j++ {
			lbls[defaultLabelName+strconv.Itoa(j)] = defaultLabelValue + strconv.Itoa(j)
		}
		samples := make([]tsdbutil.Sample, 0, maxt-mint+1)
		for t := mint; t < maxt; t++ {
			samples = append(samples, sample{t: t, v: float64(t+1)})
		}
		series[i] = newSeries(lbls, samples)
	}
	return series
}
func genSeriesOrderedPlusTwo(totalSeries, labelCount int, mint, maxt int64) []Series {
	if totalSeries == 0 || labelCount == 0 {
		return nil
	}

	series := make([]Series, totalSeries)

	for i := 0; i < totalSeries; i++ {
		lbls := make(map[string]string, labelCount)
		lbls[defaultLabelName] = strconv.Itoa(i)
		for j := 1; len(lbls) < labelCount; j++ {
			lbls[defaultLabelName+strconv.Itoa(j)] = defaultLabelValue + strconv.Itoa(j)
		}
		samples := make([]tsdbutil.Sample, 0, maxt-mint+1)
		for t := mint; t < maxt; t++ {
			samples = append(samples, sample{t: t, v: float64(t+2)})
		}
		series[i] = newSeries(lbls, samples)
	}
	return series
}
func genSeriesDevTimestamps(totalSeries, labelCount int, mint, maxt, step, devMag int64) []Series {
	if totalSeries == 0 || labelCount == 0 {
		return nil
	}

	series := make([]Series, totalSeries)

	for i := 0; i < totalSeries; i++ {
		lbls := make(map[string]string, labelCount)
		lbls[defaultLabelName] = strconv.Itoa(i)
		for j := 1; len(lbls) < labelCount; j++ {
			lbls[defaultLabelName+strconv.Itoa(j)] = defaultLabelValue + strconv.Itoa(j)
		}
		samples := make([]tsdbutil.Sample, 0, maxt-mint+1)
		for t := mint; t < maxt; t += step {
			samples = append(samples, sample{t: t + int64(rand.Float64() * float64(devMag)), v: float64(t+2)})
		}
		series[i] = newSeries(lbls, samples)
	}
	return series
}

// populateSeries generates series from given labels, mint and maxt.
func populateSeries(lbls []map[string]string, mint, maxt int64) []Series {
	if len(lbls) == 0 {
		return nil
	}

	series := make([]Series, 0, len(lbls))
	for _, lbl := range lbls {
		if len(lbl) == 0 {
			continue
		}
		samples := make([]tsdbutil.Sample, 0, maxt-mint+1)
		for t := mint; t <= maxt; t++ {
			samples = append(samples, sample{t: t, v: rand.Float64()})
		}
		series = append(series, newSeries(lbl, samples))
	}
	return series
}
