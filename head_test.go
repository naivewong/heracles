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
	// "fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
// 	"path"
// 	"path/filepath"
	// "reflect"
	"sort"
	"testing"

	// "github.com/pkg/errors"
// 	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
// 	"github.com/naivewong/tsdb-group/chunkenc"
// 	"github.com/naivewong/tsdb-group/chunks"
	"github.com/naivewong/tsdb-group/index"
	"github.com/naivewong/tsdb-group/labels"
	"github.com/naivewong/tsdb-group/testutil"
// 	"github.com/naivewong/tsdb-group/tsdbutil"
	"github.com/naivewong/tsdb-group/wal"
)

// func BenchmarkCreateSeries(b *testing.B) {
// 	series := genSeries(b.N, 10, 0, 0)

// 	h, err := NewHead(nil, nil, nil, 10000)
// 	testutil.Ok(b, err)
// 	defer h.Close()

// 	b.ReportAllocs()
// 	b.ResetTimer()

// 	for _, s := range series {
// 		h.getOrCreate(s.Labels().Hash(), s.Labels())
// 	}
// }

// func populateTestWAL(t testing.TB, w *wal.WAL, recs []interface{}) {
// 	var enc RecordEncoder
// 	for _, r := range recs {
// 		switch v := r.(type) {
// 		case []RefSeries:
// 			testutil.Ok(t, w.Log(enc.Series(v, nil)))
// 		case []RefSample:
// 			testutil.Ok(t, w.Log(enc.Samples(v, nil)))
// 		case []Stone:
// 			testutil.Ok(t, w.Log(enc.Tombstones(v, nil)))
// 		}
// 	}
// }

// func readTestWAL(t testing.TB, dir string) (recs []interface{}) {
// 	sr, err := wal.NewSegmentsReader(dir)
// 	testutil.Ok(t, err)
// 	defer sr.Close()

// 	var dec RecordDecoder
// 	r := wal.NewReader(sr)

// 	for r.Next() {
// 		rec := r.Record()

// 		switch dec.Type(rec) {
// 		case RecordSeries:
// 			series, err := dec.Series(rec, nil)
// 			testutil.Ok(t, err)
// 			recs = append(recs, series)
// 		case RecordSamples:
// 			samples, err := dec.Samples(rec, nil)
// 			testutil.Ok(t, err)
// 			recs = append(recs, samples)
// 		case RecordTombstones:
// 			tstones, err := dec.Tombstones(rec, nil)
// 			testutil.Ok(t, err)
// 			recs = append(recs, tstones)
// 		default:
// 			t.Fatalf("unknown record type")
// 		}
// 	}
// 	testutil.Ok(t, r.Err())
// 	return recs
// }

// func TestHead_ReadWAL(t *testing.T) {
// 	for _, compress := range []bool{false, true} {
// 		t.Run(fmt.Sprintf("compress=%t", compress), func(t *testing.T) {
// 			entries := []interface{}{
// 				[]RefSeries{
// 					{Ref: 10, Labels: labels.FromStrings("a", "1")},
// 					{Ref: 11, Labels: labels.FromStrings("a", "2")},
// 					{Ref: 100, Labels: labels.FromStrings("a", "3")},
// 				},
// 				[]RefSample{
// 					{Ref: 0, T: 99, V: 1},
// 					{Ref: 10, T: 100, V: 2},
// 					{Ref: 100, T: 100, V: 3},
// 				},
// 				[]RefSeries{
// 					{Ref: 50, Labels: labels.FromStrings("a", "4")},
// 					// This series has two refs pointing to it.
// 					{Ref: 101, Labels: labels.FromStrings("a", "3")},
// 				},
// 				[]RefSample{
// 					{Ref: 10, T: 101, V: 5},
// 					{Ref: 50, T: 101, V: 6},
// 					{Ref: 101, T: 101, V: 7},
// 				},
// 				[]Stone{
// 					{ref: 0, intervals: []Interval{{Mint: 99, Maxt: 101}}},
// 				},
// 			}
// 			dir, err := ioutil.TempDir("", "test_read_wal")
// 			testutil.Ok(t, err)
// 			defer func() {
// 				testutil.Ok(t, os.RemoveAll(dir))
// 			}()

// 			w, err := wal.New(nil, nil, dir, compress)
// 			testutil.Ok(t, err)
// 			defer w.Close()
// 			populateTestWAL(t, w, entries)

// 			head, err := NewHead(nil, nil, w, 1000)
// 			testutil.Ok(t, err)

// 			testutil.Ok(t, head.Init(math.MinInt64))
// 			testutil.Equals(t, uint64(101), head.lastSeriesID)

// 			s10 := head.series.getByID(10)
// 			s11 := head.series.getByID(11)
// 			s50 := head.series.getByID(50)
// 			s100 := head.series.getByID(100)

// 			testutil.Equals(t, labels.FromStrings("a", "1"), s10.lset)
// 			testutil.Equals(t, (*memSeries)(nil), s11) // Series without samples should be garbage colected at head.Init().
// 			testutil.Equals(t, labels.FromStrings("a", "4"), s50.lset)
// 			testutil.Equals(t, labels.FromStrings("a", "3"), s100.lset)

// 			expandChunk := func(c chunkenc.Iterator) (x []sample) {
// 				for c.Next() {
// 					t, v := c.At()
// 					x = append(x, sample{t: t, v: v})
// 				}
// 				testutil.Ok(t, c.Err())
// 				return x
// 			}
// 			testutil.Equals(t, []sample{{100, 2}, {101, 5}}, expandChunk(s10.iterator(0)))
// 			testutil.Equals(t, []sample{{101, 6}}, expandChunk(s50.iterator(0)))
// 			testutil.Equals(t, []sample{{100, 3}, {101, 7}}, expandChunk(s100.iterator(0)))
// 		})
// 	}
// }

// func TestHead_WALMultiRef(t *testing.T) {
// 	dir, err := ioutil.TempDir("", "test_wal_multi_ref")
// 	testutil.Ok(t, err)
// 	defer func() {
// 		testutil.Ok(t, os.RemoveAll(dir))
// 	}()

// 	w, err := wal.New(nil, nil, dir, false)
// 	testutil.Ok(t, err)

// 	head, err := NewHead(nil, nil, w, 1000)
// 	testutil.Ok(t, err)

// 	testutil.Ok(t, head.Init(0))
// 	app := head.Appender()
// 	ref1, err := app.Add(labels.FromStrings("foo", "bar"), 100, 1)
// 	testutil.Ok(t, err)
// 	testutil.Ok(t, app.Commit())

// 	testutil.Ok(t, head.Truncate(200))

// 	app = head.Appender()
// 	ref2, err := app.Add(labels.FromStrings("foo", "bar"), 300, 2)
// 	testutil.Ok(t, err)
// 	testutil.Ok(t, app.Commit())

// 	if ref1 == ref2 {
// 		t.Fatal("Refs are the same")
// 	}
// 	testutil.Ok(t, head.Close())

// 	w, err = wal.New(nil, nil, dir, false)
// 	testutil.Ok(t, err)

// 	head, err = NewHead(nil, nil, w, 1000)
// 	testutil.Ok(t, err)
// 	testutil.Ok(t, head.Init(0))
// 	defer head.Close()

// 	q, err := NewBlockQuerier(head, 0, 300)
// 	testutil.Ok(t, err)
// 	series := query(t, q, labels.NewEqualMatcher("foo", "bar"))
// 	testutil.Equals(t, map[string][]tsdbutil.Sample{`{foo="bar"}`: {sample{100, 1}, sample{300, 2}}}, series)
// }

// func TestHead_Truncate(t *testing.T) {
// 	h, err := NewHead(nil, nil, nil, 1000)
// 	testutil.Ok(t, err)
// 	defer h.Close()

// 	h.initTime(0)

// 	s1, _ := h.getOrCreate(1, labels.FromStrings("a", "1", "b", "1"))
// 	s2, _ := h.getOrCreate(2, labels.FromStrings("a", "2", "b", "1"))
// 	s3, _ := h.getOrCreate(3, labels.FromStrings("a", "1", "b", "2"))
// 	s4, _ := h.getOrCreate(4, labels.FromStrings("a", "2", "b", "2", "c", "1"))

// 	s1.chunks = []*memChunk{
// 		{minTime: 0, maxTime: 999},
// 		{minTime: 1000, maxTime: 1999},
// 		{minTime: 2000, maxTime: 2999},
// 	}
// 	s2.chunks = []*memChunk{
// 		{minTime: 1000, maxTime: 1999},
// 		{minTime: 2000, maxTime: 2999},
// 		{minTime: 3000, maxTime: 3999},
// 	}
// 	s3.chunks = []*memChunk{
// 		{minTime: 0, maxTime: 999},
// 		{minTime: 1000, maxTime: 1999},
// 	}
// 	s4.chunks = []*memChunk{}

// 	// Truncation need not be aligned.
// 	testutil.Ok(t, h.Truncate(1))

// 	testutil.Ok(t, h.Truncate(2000))

// 	testutil.Equals(t, []*memChunk{
// 		{minTime: 2000, maxTime: 2999},
// 	}, h.series.getByID(s1.ref).chunks)

// 	testutil.Equals(t, []*memChunk{
// 		{minTime: 2000, maxTime: 2999},
// 		{minTime: 3000, maxTime: 3999},
// 	}, h.series.getByID(s2.ref).chunks)

// 	testutil.Assert(t, h.series.getByID(s3.ref) == nil, "")
// 	testutil.Assert(t, h.series.getByID(s4.ref) == nil, "")

// 	postingsA1, _ := index.ExpandPostings(h.postings.Get("a", "1"))
// 	postingsA2, _ := index.ExpandPostings(h.postings.Get("a", "2"))
// 	postingsB1, _ := index.ExpandPostings(h.postings.Get("b", "1"))
// 	postingsB2, _ := index.ExpandPostings(h.postings.Get("b", "2"))
// 	postingsC1, _ := index.ExpandPostings(h.postings.Get("c", "1"))
// 	postingsAll, _ := index.ExpandPostings(h.postings.Get("", ""))

// 	testutil.Equals(t, []uint64{s1.ref}, postingsA1)
// 	testutil.Equals(t, []uint64{s2.ref}, postingsA2)
// 	testutil.Equals(t, []uint64{s1.ref, s2.ref}, postingsB1)
// 	testutil.Equals(t, []uint64{s1.ref, s2.ref}, postingsAll)
// 	testutil.Assert(t, postingsB2 == nil, "")
// 	testutil.Assert(t, postingsC1 == nil, "")

// 	testutil.Equals(t, map[string]struct{}{
// 		"":  {}, // from 'all' postings list
// 		"a": {},
// 		"b": {},
// 		"1": {},
// 		"2": {},
// 	}, h.symbols)

// 	testutil.Equals(t, map[string]stringset{
// 		"a": {"1": struct{}{}, "2": struct{}{}},
// 		"b": {"1": struct{}{}},
// 		"":  {"": struct{}{}},
// 	}, h.values)
// }

func TestStripeSeries(test *testing.T) {
	s := newStripeSeries()

	// Add some groups.
	s.addGroup([]labels.Labels{
		labels.Labels{labels.Label{Name: "a", Value: "1"}, labels.Label{Name: "b", Value: "1"}},
		labels.Labels{labels.Label{Name: "a", Value: "1"}, labels.Label{Name: "b", Value: "2"}},
		labels.Labels{labels.Label{Name: "a", Value: "1"}, labels.Label{Name: "b", Value: "3"}}}, 0, []uint64{100, 101, 102})
	s.addGroup([]labels.Labels{
		labels.Labels{labels.Label{Name: "c", Value: "1"}, labels.Label{Name: "d", Value: "1"}},
		labels.Labels{labels.Label{Name: "c", Value: "1"}, labels.Label{Name: "d", Value: "2"}},
		labels.Labels{labels.Label{Name: "c", Value: "1"}, labels.Label{Name: "d", Value: "3"}}}, 1, []uint64{200, 201, 202})
	s.addGroup([]labels.Labels{
		labels.Labels{labels.Label{Name: "e", Value: "1"}, labels.Label{Name: "f", Value: "1"}},
		labels.Labels{labels.Label{Name: "e", Value: "1"}, labels.Label{Name: "f", Value: "2"}},
		labels.Labels{labels.Label{Name: "e", Value: "1"}, labels.Label{Name: "f", Value: "3"}}}, 2, []uint64{300, 301, 302})

	// Append some data.
	gms1 := s.getByID(100)
	gms2 := s.getByID(200)
	gms3 := s.getByID(300)
	data := [][]float64{}
	for i := 0; i < 50; i++ {
		temp := []float64{rand.Float64(), rand.Float64(), rand.Float64()}
		gms1.appendGroup(int64(i * 1000), temp)
		gms2.appendGroup(int64(i * 1000 + 100000), temp)
		gms3.appendGroup(int64(i * 1000 + 200000), temp)
		data = append(data, temp)
	}

	// First group gc.
	rmSeries, rmGroups := s.gc(25000)
	testutil.Equals(test, 0, len(rmSeries))
	testutil.Equals(test, 0, rmGroups)
	it := gms1.iterator(0, 100)
	i := 25
	for it.Next() {
		t, v := it.At()
		testutil.Equals(test, int64(i * 1000), t)
		testutil.Equals(test, data[i][0], v)
		i += 1
	}
	testutil.Equals(test, 50, i)

	// First group gc.
	rmSeries, rmGroups = s.gc(50000)
	testutil.Equals(test, 3, len(rmSeries))
	_, ok := rmSeries[100]
	testutil.Equals(test, true, ok)
	_, ok = rmSeries[101]
	testutil.Equals(test, true, ok)
	_, ok = rmSeries[102]
	testutil.Equals(test, true, ok)
	testutil.Equals(test, 1, rmGroups)
	it = gms1.iterator(0, 100)
	testutil.Equals(test, false, it.Next())

	// Second and third group.
	rmSeries, rmGroups = s.gc(250000)
	testutil.Equals(test, 6, len(rmSeries))
	testutil.Equals(test, 2, rmGroups)
}

func TestMemSeries_truncateBefore(test *testing.T) {
	s := newMemSeries([]labels.Labels{labels.FromStrings("a", "b"), labels.FromStrings("a", "c")}, 1, []uint64{11,12})

	for i := 0; i < 4000; i += 5 {
		ok := s.appendGroup(int64(i), []float64{float64(i), float64(i)})
		testutil.Assert(test, ok == true, "sample append failed")
	}

	testutil.Assert(test, s.chunk(11) != nil, "")

	removed := s.truncateBefore(2000)

	testutil.Equals(test, int64(2000), s.minTime())
	testutil.Equals(test, int64(3995), s.maxTime())
	testutil.Equals(test, 800, removed)

	it := s.iterator(2500, 11)
	i := 2500
	for it.Next() {
		t, v := it.At()
		testutil.Equals(test, int64(i), t)
		testutil.Equals(test, float64(i), v)
		i += 5
	}
	testutil.Equals(test, 4000, i)
}

func TestHeadWithoutWAL(test *testing.T) {
	numPoints1 := 1000
	numPoints2 := 1000
	numSeries1 := 3
	numSeries2 := 3
	timestamps1 := make([]int64, 0, numPoints1)
	timestamps2 := make([]int64, 0, numPoints2)
	data1 := make([][]float64, 0, numPoints1)
	data2 := make([][]float64, 0, numPoints2)
	for i := 0; i < numPoints1; i++ {
		timestamps1 = append(timestamps1, int64(i))
		temp := make([]float64, 0, numSeries1)
		for j := 0; j < numSeries1; j++ {
			temp = append(temp, rand.Float64())
		}
		data1 = append(data1, temp)
	}
	for i := 0; i < numPoints2; i++ {
		timestamps2 = append(timestamps2, int64(i + 1000))
		temp := make([]float64, 0, numSeries2)
		for j := 0; j < numSeries2; j++ {
			temp = append(temp, rand.Float64())
		}
		data2 = append(data2, temp)
	}

	h, err := NewHead(nil, nil, nil, 120 * 3600)
	testutil.Ok(test, err)
	err = h.Init(math.MinInt64)
	testutil.Ok(test, err)

	app := h.Appender()
	labels1 := []labels.Labels{{{"alec", "wang1"}}, {{"alec", "wang2"}}, {{"alec", "wang3"}}}
	for i := 0; i < numPoints1; i++ {
		ref, err := app.AddGroup(labels1, timestamps1[i], data1[i])
		testutil.Ok(test, err)
		testutil.Equals(test, uint64(1), ref)
	}
	err = app.Commit()
	testutil.Ok(test, err)
	labels2 := []labels.Labels{{{"a", "1"}, {"b", "1"}}, {{"a", "1"}, {"b", "2"}}, {{"a", "1"}, {"b", "3"}}}
	for i := 0; i < numPoints2; i++ {
		ref, err := app.AddGroup(labels2, timestamps2[i], data2[i])
		testutil.Ok(test, err)
		testutil.Equals(test, uint64(2), ref)
	}
	err = app.Commit()
	testutil.Ok(test, err)

	// Test head truncate.
	err = h.Truncate(100)
	testutil.Ok(test, err)

	// Test block querier (Seek()).
	q, err := NewBlockQuerier(h, 0, 100000)
	testutil.Ok(test, err)
	ss, err := q.Select(labels.NewEqualMatcher("alec", "wang1"))
	testutil.Ok(test, err)
	testutil.Equals(test, true, ss.Next()) // Next Series.
	s := ss.At()
	testutil.Equals(test, true, s.Labels().Equals(labels.Labels{{"alec", "wang1"}})) // Labels.
	it := s.Iterator() // SeriesIterator.
	testutil.Equals(test, true, it.Seek(487))
	i := 487
	t, v := it.At()
	testutil.Equals(test, int64(i), t)
	testutil.Equals(test, data1[i][0], v)
	i += 1
	for it.Next() {
		t, v = it.At()
		testutil.Equals(test, int64(i), t)
		testutil.Equals(test, data1[i][0], v)
		i += 1
	}
	testutil.Equals(test, numPoints1, i)

	// Test block querier (Next() whole range).
	q, err = NewBlockQuerier(h, 0, 100000)
	testutil.Ok(test, err)
	ss, err = q.Select(labels.NewEqualMatcher("alec", "wang1"))
	testutil.Ok(test, err)
	testutil.Equals(test, true, ss.Next()) // Next Series.
	s = ss.At()
	testutil.Equals(test, true, s.Labels().Equals(labels.Labels{{"alec", "wang1"}})) // Labels.
	it = s.Iterator() // SeriesIterator.
	i = 100
	for it.Next() {
		t, v = it.At()
		testutil.Equals(test, int64(i), t)
		testutil.Equals(test, data1[i][0], v)
		i += 1
	}
	testutil.Equals(test, numPoints1, i)

	// Test block querier (Next() for partial range).
	q, err = NewBlockQuerier(h, 1400, 100000)
	testutil.Ok(test, err)
	ss, err = q.Select(labels.NewEqualMatcher("a", "1"))
	testutil.Ok(test, err)
	testutil.Equals(test, true, ss.Next()) // Next Series.
	s = ss.At()
	testutil.Equals(test, true, s.Labels().Equals(labels.Labels{{"a", "1"}, {"b", "1"}})) // Labels.
	it = s.Iterator() // SeriesIterator.
	i = 1400
	for it.Next() {
		t, v = it.At()
		testutil.Equals(test, int64(i), t)
		testutil.Equals(test, data2[i-1000][0], v)
		i += 1
	}
	testutil.Equals(test, numPoints2, i-1000)
}

func prepareWAL(numPoints int, dir string) (*wal.WAL, error) {
	gseries := []RefGroupSeries{
		{0, []RefSeries{
			{0, labels.Labels{{"__name__", "cpu"}, {"host", "1"}}},
			{1, labels.Labels{{"__name__", "disk"}, {"host", "1"}}},
			{2, labels.Labels{{"__name__", "memory"}, {"host", "1"}}},
			{3, labels.Labels{{"__name__", "network"}, {"host", "1"}}},
		}},
		{1, []RefSeries{
			{4, labels.Labels{{"__name__", "cpu"}, {"host", "2"}}},
			{5, labels.Labels{{"__name__", "disk"}, {"host", "2"}}},
			{6, labels.Labels{{"__name__", "memory"}, {"host", "2"}}},
			{7, labels.Labels{{"__name__", "network"}, {"host", "2"}}},
		}},
		{2, []RefSeries{
			{8, labels.Labels{{"__name__", "cpu"}, {"host", "3"}}},
			{9, labels.Labels{{"__name__", "disk"}, {"host", "3"}}},
			{10, labels.Labels{{"__name__", "memory"}, {"host", "3"}}},
			{11, labels.Labels{{"__name__", "network"}, {"host", "3"}}},
		}},
		{3, []RefSeries{
			{12, labels.Labels{{"__name__", "cpu"}, {"host", "4"}}},
			{13, labels.Labels{{"__name__", "disk"}, {"host", "4"}}},
			{14, labels.Labels{{"__name__", "memory"}, {"host", "4"}}},
			{15, labels.Labels{{"__name__", "network"}, {"host", "4"}}},
		}},
	}
	gsamples := []RefGroupSample{}
	for i := 0; i < numPoints; i++ {
		for j := 0; j < len(gseries); j++ {
			gsamples = append(gsamples, RefGroupSample{
				GroupRef: uint64(j),
				T:        int64(i),
				Ids:      []uint64{uint64(j*len(gseries)), uint64(j*len(gseries)+1), uint64(j*len(gseries)+2), uint64(j*len(gseries)+3)},
				Vals:     []float64{1.2, 1.3, 1.4, 1.5},
			})
		}
	}
	stones := []Stone{
		{ref: 0, intervals: Intervals{
			{Mint: 10, Maxt: 20},
		}},
		{ref: 1, intervals: Intervals{
			{Mint: 20, Maxt: 30},
		}},
		{ref: 2, intervals: Intervals{
			{Mint: 30, Maxt: 40},
		}},
		{ref: 3, intervals: Intervals{
			{Mint: 40, Maxt: 50},
		}},
	}

	w, err := wal.NewSize(nil, nil, dir, 64 * 1024, false)
	if err != nil {
		return nil, err
	}

	var enc RecordEncoder
	var buf []byte
	buf = enc.GroupSeries(gseries, nil)
	err = w.Log(buf[:])
	if err != nil {
		return nil, err
	}
	buf = enc.GroupSamples(gsamples, nil)
	err = w.Log(buf[:])
	if err != nil {
		return nil, err
	}
	buf = enc.GroupTombstones(stones, nil)
	err = w.Log(buf[:])
	if err != nil {
		return nil, err
	}

	// sr, err := wal.NewSegmentsReader(dir)
	// if err != nil {
	// 	return nil, err
	// }
	// r := wal.NewReader(sr)
	// var dec RecordDecoder
	// for r.Next() {
	// 	rec := r.Record()

	// 	switch dec.Type(rec) {
	// 	case RecordGroupSeries:
	// 		series, err := dec.GroupSeries(rec, nil)
	// 		if err != nil {
	// 			return nil, err
	// 		}
	// 		if !reflect.DeepEqual(gseries, series) {
	// 			fmt.Println(series)
	// 			return nil, errors.Errorf("series not equal")
	// 		}
	// 	case RecordGroupSamples:
	// 		samples, err := dec.GroupSamples(rec, nil)
	// 		if err != nil {
	// 			return nil, err
	// 		}
	// 		if !reflect.DeepEqual(gsamples, samples) {
	// 			return nil, errors.Errorf("samples not equal")
	// 		}
	// 	case RecordGroupTombstones:
	// 		tstones, err := dec.GroupTombstones(rec, nil)
	// 		if err != nil {
	// 			return nil, err
	// 		}
	// 		if !reflect.DeepEqual(stones, tstones) {
	// 			return nil, errors.Errorf("stones not equal")
	// 		}
	// 	}
	// }

	return w, nil
}

func TestHeadWithWAL(test *testing.T) {
	tmpdir, err := ioutil.TempDir("", "head_with_wal")
	testutil.Ok(test, err)
	defer func() {
		testutil.Ok(test, os.RemoveAll(tmpdir))
	}()

	numPoints := 1000
	w, err := prepareWAL(numPoints, tmpdir)
	testutil.Ok(test, err)

	h, err := NewHead(nil, nil, w, 120 * 3600)
	testutil.Ok(test, err)
	err = h.Init(math.MinInt64)
	testutil.Ok(test, err)

	// Test head index reader.
	ir, _ := h.Index()
	{
		// GroupPostings and SortedGroupPostings for index.AllGroupPostings.
		p, err := ir.GroupPostings(index.AllGroupPostings)
		testutil.Ok(test, err)
		arr, err := index.ExpandPostings(p)
		testutil.Ok(test, err)
		sort.Slice(arr, func(i, j int) bool { return arr[i] < arr[j] })
		mockGroups := []uint64{0, 1, 2, 3}
		testutil.Equals(test, mockGroups, arr)

		p, err = ir.GroupPostings(index.AllGroupPostings)
		testutil.Ok(test, err)
		p = ir.SortedGroupPostings(p)
		arr, err = index.ExpandPostings(p)
		testutil.Ok(test, err)
		testutil.Equals(test, mockGroups, arr)
	}
	{
		// Group 0.
		p, err := ir.GroupPostings(0)
		testutil.Ok(test, err)
		arr, err := index.ExpandPostings(p)
		testutil.Ok(test, err)
		testutil.Equals(test, []uint64{0, 1, 2, 3}, arr)
	}
	{
		// Group 1.
		p, err := ir.GroupPostings(1)
		testutil.Ok(test, err)
		arr, err := index.ExpandPostings(p)
		testutil.Ok(test, err)
		testutil.Equals(test, []uint64{4, 5, 6, 7}, arr)
	}
	{
		// Group 2.
		p, err := ir.GroupPostings(2)
		testutil.Ok(test, err)
		arr, err := index.ExpandPostings(p)
		testutil.Ok(test, err)
		testutil.Equals(test, []uint64{8, 9, 10, 11}, arr)
	}
	{
		// Group 3.
		p, err := ir.GroupPostings(3)
		testutil.Ok(test, err)
		arr, err := index.ExpandPostings(p)
		testutil.Ok(test, err)
		testutil.Equals(test, []uint64{12, 13, 14, 15}, arr)
	}

	// Test block querier.
	{
		q, err := NewBlockQuerier(h, 0, 100000)
		testutil.Ok(test, err)
		ss, err := q.Select(labels.NewEqualMatcher("host", "1"))
		testutil.Ok(test, err)
		{
			testutil.Equals(test, true, ss.Next()) // Next Series.
			s := ss.At()
			testutil.Equals(test, true, s.Labels().Equals(labels.Labels{{"__name__", "cpu"}, {"host", "1"}})) // Labels.
			it := s.Iterator() // SeriesIterator.
			testutil.Equals(test, true, it.Seek(15))
			i := 21
			t, v := it.At()
			testutil.Equals(test, int64(i), t)
			testutil.Equals(test, 1.2, v)
			i += 1
			for it.Next() {
				t, v := it.At()
				testutil.Equals(test, int64(i), t)
				testutil.Equals(test, 1.2, v)
				i += 1
			}
			testutil.Equals(test, numPoints, i)
		}
		{
			testutil.Equals(test, true, ss.Next()) // Next Series.
			s := ss.At()
			testutil.Equals(test, true, s.Labels().Equals(labels.Labels{{"__name__", "disk"}, {"host", "1"}})) // Labels.
			it := s.Iterator() // SeriesIterator.
			testutil.Equals(test, true, it.Seek(50))
			i := 50
			t, v := it.At()
			testutil.Equals(test, int64(i), t)
			testutil.Equals(test, 1.3, v)
			i += 1
			for it.Next() {
				t, v := it.At()
				testutil.Equals(test, int64(i), t)
				testutil.Equals(test, 1.3, v)
				i += 1
			}
			testutil.Equals(test, numPoints, i)
		}
	}
	{
		q, err := NewBlockQuerier(h, 0, 100000)
		testutil.Ok(test, err)
		matcher, err := labels.NewRegexpMatcher("__name__", "cpu")
		testutil.Ok(test, err)
		ss, err := q.Select(matcher)
		testutil.Ok(test, err)
		{
			// First series.
			testutil.Equals(test, true, ss.Next()) // Next Series.
			s := ss.At()
			testutil.Equals(test, true, s.Labels().Equals(labels.Labels{{"__name__", "cpu"}, {"host", "1"}})) // Labels.
			it := s.Iterator() // SeriesIterator.
			testutil.Equals(test, true, it.Seek(50))
			i := 50
			t, v := it.At()
			testutil.Equals(test, int64(i), t)
			testutil.Equals(test, 1.2, v)
			i += 1
			for it.Next() {
				t, v := it.At()
				testutil.Equals(test, int64(i), t)
				testutil.Equals(test, 1.2, v)
				i += 1
			}
			testutil.Equals(test, numPoints, i)
		}
		{
			// Second series.
			testutil.Equals(test, true, ss.Next()) // Next Series.
			s := ss.At()
			testutil.Equals(test, true, s.Labels().Equals(labels.Labels{{"__name__", "cpu"}, {"host", "2"}})) // Labels.
			it := s.Iterator() // SeriesIterator.
			testutil.Equals(test, true, it.Seek(25))
			i := 31
			t, v := it.At()
			testutil.Equals(test, int64(i), t)
			testutil.Equals(test, 1.2, v)
			i += 1
			for it.Next() {
				t, v := it.At()
				testutil.Equals(test, int64(i), t)
				testutil.Equals(test, 1.2, v)
				i += 1
			}
			testutil.Equals(test, numPoints, i)
		}
	}

	// Delete group 0.
	err = h.Delete(0, 100000, labels.NewEqualMatcher("host", "1"))
	testutil.Ok(test, err)

	{
		q, err := NewBlockQuerier(h, 0, 100000)
		testutil.Ok(test, err)

		// Cannot get group 0 now.
		ss, err := q.Select(labels.NewEqualMatcher("host", "1"))
		testutil.Ok(test, err)
		testutil.Equals(test, false, ss.Next())

		matcher, err := labels.NewRegexpMatcher("host", "[3-4]")
		testutil.Ok(test, err)
		ss, err = q.Select(matcher)
		testutil.Ok(test, err)

		{
			// First series.
			testutil.Equals(test, true, ss.Next()) // Next Series.
			s := ss.At()
			testutil.Equals(test, true, s.Labels().Equals(labels.Labels{{"__name__", "cpu"}, {"host", "3"}})) // Labels.
			it := s.Iterator() // SeriesIterator.
			i := 0
			for it.Next() {
				t, v := it.At()
				testutil.Equals(test, int64(i), t)
				testutil.Equals(test, 1.2, v)
				i += 1
				if i == 30 {
					i = 41
				}
			}
			testutil.Equals(test, numPoints, i)
		}
		{
			// Second series.
			testutil.Equals(test, true, ss.Next()) // Next Series.
			s := ss.At()
			testutil.Equals(test, true, s.Labels().Equals(labels.Labels{{"__name__", "cpu"}, {"host", "4"}})) // Labels.
			it := s.Iterator() // SeriesIterator.
			i := 0
			for it.Next() {
				t, v := it.At()
				testutil.Equals(test, int64(i), t)
				testutil.Equals(test, 1.2, v)
				i += 1
				if i == 40 {
					i = 51
				}
			}
			testutil.Equals(test, numPoints, i)
		}
	}
}

// func TestHeadDeleteSeriesWithoutSamples(t *testing.T) {
// 	for _, compress := range []bool{false, true} {
// 		t.Run(fmt.Sprintf("compress=%t", compress), func(t *testing.T) {
// 			entries := []interface{}{
// 				[]RefSeries{
// 					{Ref: 10, Labels: labels.FromStrings("a", "1")},
// 				},
// 				[]RefSample{},
// 				[]RefSeries{
// 					{Ref: 50, Labels: labels.FromStrings("a", "2")},
// 				},
// 				[]RefSample{
// 					{Ref: 50, T: 80, V: 1},
// 					{Ref: 50, T: 90, V: 1},
// 				},
// 			}
// 			dir, err := ioutil.TempDir("", "test_delete_series")
// 			testutil.Ok(t, err)
// 			defer func() {
// 				testutil.Ok(t, os.RemoveAll(dir))
// 			}()

// 			w, err := wal.New(nil, nil, dir, compress)
// 			testutil.Ok(t, err)
// 			defer w.Close()
// 			populateTestWAL(t, w, entries)

// 			head, err := NewHead(nil, nil, w, 1000)
// 			testutil.Ok(t, err)

// 			testutil.Ok(t, head.Init(math.MinInt64))

// 			testutil.Ok(t, head.Delete(0, 100, labels.NewEqualMatcher("a", "1")))
// 		})
// 	}
// }

// func TestHeadDeleteSimple(t *testing.T) {
// 	buildSmpls := func(s []int64) []sample {
// 		ss := make([]sample, 0, len(s))
// 		for _, t := range s {
// 			ss = append(ss, sample{t: t, v: float64(t)})
// 		}
// 		return ss
// 	}
// 	smplsAll := buildSmpls([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
// 	lblDefault := labels.Label{"a", "b"}

// 	cases := []struct {
// 		dranges  Intervals
// 		smplsExp []sample
// 	}{
// 		{
// 			dranges:  Intervals{{0, 3}},
// 			smplsExp: buildSmpls([]int64{4, 5, 6, 7, 8, 9}),
// 		},
// 		{
// 			dranges:  Intervals{{1, 3}},
// 			smplsExp: buildSmpls([]int64{0, 4, 5, 6, 7, 8, 9}),
// 		},
// 		{
// 			dranges:  Intervals{{1, 3}, {4, 7}},
// 			smplsExp: buildSmpls([]int64{0, 8, 9}),
// 		},
// 		{
// 			dranges:  Intervals{{1, 3}, {4, 700}},
// 			smplsExp: buildSmpls([]int64{0}),
// 		},
// 		{ // This case is to ensure that labels and symbols are deleted.
// 			dranges:  Intervals{{0, 9}},
// 			smplsExp: buildSmpls([]int64{}),
// 		},
// 	}

// 	for _, compress := range []bool{false, true} {
// 		t.Run(fmt.Sprintf("compress=%t", compress), func(t *testing.T) {
// 		Outer:
// 			for _, c := range cases {
// 				dir, err := ioutil.TempDir("", "test_wal_reload")
// 				testutil.Ok(t, err)
// 				defer func() {
// 					testutil.Ok(t, os.RemoveAll(dir))
// 				}()

// 				w, err := wal.New(nil, nil, path.Join(dir, "wal"), compress)
// 				testutil.Ok(t, err)
// 				defer w.Close()

// 				head, err := NewHead(nil, nil, w, 1000)
// 				testutil.Ok(t, err)
// 				defer head.Close()

// 				app := head.Appender()
// 				for _, smpl := range smplsAll {
// 					_, err = app.Add(labels.Labels{lblDefault}, smpl.t, smpl.v)
// 					testutil.Ok(t, err)

// 				}
// 				testutil.Ok(t, app.Commit())

// 				// Delete the ranges.
// 				for _, r := range c.dranges {
// 					testutil.Ok(t, head.Delete(r.Mint, r.Maxt, labels.NewEqualMatcher(lblDefault.Name, lblDefault.Value)))
// 				}

// 				// Compare the samples for both heads - before and after the reload.
// 				reloadedW, err := wal.New(nil, nil, w.Dir(), compress) // Use a new wal to ensure deleted samples are gone even after a reload.
// 				testutil.Ok(t, err)
// 				defer reloadedW.Close()
// 				reloadedHead, err := NewHead(nil, nil, reloadedW, 1000)
// 				testutil.Ok(t, err)
// 				defer reloadedHead.Close()
// 				testutil.Ok(t, reloadedHead.Init(0))
// 				for _, h := range []*Head{head, reloadedHead} {
// 					indexr, err := h.Index()
// 					testutil.Ok(t, err)
// 					// Use an emptyTombstoneReader explicitly to get all the samples.
// 					css, err := LookupChunkSeries(indexr, emptyTombstoneReader, labels.NewEqualMatcher(lblDefault.Name, lblDefault.Value))
// 					testutil.Ok(t, err)

// 					// Getting the actual samples.
// 					actSamples := make([]sample, 0)
// 					for css.Next() {
// 						lblsAct, chkMetas, intv := css.At()
// 						testutil.Equals(t, labels.Labels{lblDefault}, lblsAct)
// 						testutil.Equals(t, 0, len(intv))

// 						chunkr, err := h.Chunks()
// 						testutil.Ok(t, err)
// 						for _, meta := range chkMetas {
// 							chk, err := chunkr.Chunk(meta.Ref)
// 							testutil.Ok(t, err)
// 							ii := chk.Iterator()
// 							for ii.Next() {
// 								t, v := ii.At()
// 								actSamples = append(actSamples, sample{t: t, v: v})
// 							}
// 						}
// 					}

// 					testutil.Ok(t, css.Err())
// 					testutil.Equals(t, c.smplsExp, actSamples)
// 				}

// 				// Compare the query results for both heads - before and after the reload.
// 				expSeriesSet := newMockSeriesSet([]Series{
// 					newSeries(map[string]string{lblDefault.Name: lblDefault.Value}, func() []tsdbutil.Sample {
// 						ss := make([]tsdbutil.Sample, 0, len(c.smplsExp))
// 						for _, s := range c.smplsExp {
// 							ss = append(ss, s)
// 						}
// 						return ss
// 					}(),
// 					),
// 				})
// 				for _, h := range []*Head{head, reloadedHead} {
// 					q, err := NewBlockQuerier(h, h.MinTime(), h.MaxTime())
// 					testutil.Ok(t, err)
// 					actSeriesSet, err := q.Select(labels.NewEqualMatcher(lblDefault.Name, lblDefault.Value))
// 					testutil.Ok(t, err)

// 					lns, err := q.LabelNames()
// 					testutil.Ok(t, err)
// 					lvs, err := q.LabelValues(lblDefault.Name)
// 					testutil.Ok(t, err)
// 					// When all samples are deleted we expect that no labels should exist either.
// 					if len(c.smplsExp) == 0 {
// 						testutil.Equals(t, 0, len(lns))
// 						testutil.Equals(t, 0, len(lvs))
// 						testutil.Assert(t, actSeriesSet.Next() == false, "")
// 						testutil.Ok(t, h.Close())
// 						continue
// 					} else {
// 						testutil.Equals(t, 1, len(lns))
// 						testutil.Equals(t, 1, len(lvs))
// 						testutil.Equals(t, lblDefault.Name, lns[0])
// 						testutil.Equals(t, lblDefault.Value, lvs[0])
// 					}

// 					for {
// 						eok, rok := expSeriesSet.Next(), actSeriesSet.Next()
// 						testutil.Equals(t, eok, rok)

// 						if !eok {
// 							testutil.Ok(t, h.Close())
// 							continue Outer
// 						}
// 						expSeries := expSeriesSet.At()
// 						actSeries := actSeriesSet.At()

// 						testutil.Equals(t, expSeries.Labels(), actSeries.Labels())

// 						smplExp, errExp := expandSeriesIterator(expSeries.Iterator())
// 						smplRes, errRes := expandSeriesIterator(actSeries.Iterator())

// 						testutil.Equals(t, errExp, errRes)
// 						testutil.Equals(t, smplExp, smplRes)
// 					}
// 				}
// 			}
// 		})
// 	}
// }

// func TestDeleteUntilCurMax(t *testing.T) {
// 	numSamples := int64(10)
// 	hb, err := NewHead(nil, nil, nil, 1000000)
// 	testutil.Ok(t, err)
// 	defer hb.Close()
// 	app := hb.Appender()
// 	smpls := make([]float64, numSamples)
// 	for i := int64(0); i < numSamples; i++ {
// 		smpls[i] = rand.Float64()
// 		_, err := app.Add(labels.Labels{{"a", "b"}}, i, smpls[i])
// 		testutil.Ok(t, err)
// 	}
// 	testutil.Ok(t, app.Commit())
// 	testutil.Ok(t, hb.Delete(0, 10000, labels.NewEqualMatcher("a", "b")))

// 	// Test the series have been deleted.
// 	q, err := NewBlockQuerier(hb, 0, 100000)
// 	testutil.Ok(t, err)
// 	res, err := q.Select(labels.NewEqualMatcher("a", "b"))
// 	testutil.Ok(t, err)
// 	testutil.Assert(t, !res.Next(), "series didn't get deleted")

// 	// Add again and test for presence.
// 	app = hb.Appender()
// 	_, err = app.Add(labels.Labels{{"a", "b"}}, 11, 1)
// 	testutil.Ok(t, err)
// 	testutil.Ok(t, app.Commit())
// 	q, err = NewBlockQuerier(hb, 0, 100000)
// 	testutil.Ok(t, err)
// 	res, err = q.Select(labels.NewEqualMatcher("a", "b"))
// 	testutil.Ok(t, err)
// 	testutil.Assert(t, res.Next(), "series don't exist")
// 	exps := res.At()
// 	it := exps.Iterator()
// 	ressmpls, err := expandSeriesIterator(it)
// 	testutil.Ok(t, err)
// 	testutil.Equals(t, []tsdbutil.Sample{sample{11, 1}}, ressmpls)
// }

// func TestDeletedSamplesAndSeriesStillInWALAfterCheckpoint(t *testing.T) {
// 	dir, err := ioutil.TempDir("", "test_delete_wal")
// 	testutil.Ok(t, err)
// 	defer func() {
// 		testutil.Ok(t, os.RemoveAll(dir))
// 	}()
// 	wlog, err := wal.NewSize(nil, nil, dir, 32768, false)
// 	testutil.Ok(t, err)

// 	// Enough samples to cause a checkpoint.
// 	numSamples := 10000
// 	hb, err := NewHead(nil, nil, wlog, int64(numSamples)*10)
// 	testutil.Ok(t, err)
// 	defer hb.Close()
// 	for i := 0; i < numSamples; i++ {
// 		app := hb.Appender()
// 		_, err := app.Add(labels.Labels{{"a", "b"}}, int64(i), 0)
// 		testutil.Ok(t, err)
// 		testutil.Ok(t, app.Commit())
// 	}
// 	testutil.Ok(t, hb.Delete(0, int64(numSamples), labels.NewEqualMatcher("a", "b")))
// 	testutil.Ok(t, hb.Truncate(1))
// 	testutil.Ok(t, hb.Close())

// 	// Confirm there's been a checkpoint.
// 	cdir, _, err := LastCheckpoint(dir)
// 	testutil.Ok(t, err)
// 	// Read in checkpoint and WAL.
// 	recs := readTestWAL(t, cdir)
// 	recs = append(recs, readTestWAL(t, dir)...)

// 	var series, samples, stones int
// 	for _, rec := range recs {
// 		switch rec.(type) {
// 		case []RefSeries:
// 			series++
// 		case []RefSample:
// 			samples++
// 		case []Stone:
// 			stones++
// 		default:
// 			t.Fatalf("unknown record type")
// 		}
// 	}
// 	testutil.Equals(t, 1, series)
// 	testutil.Equals(t, 9999, samples)
// 	testutil.Equals(t, 1, stones)

// }

// func TestDelete_e2e(t *testing.T) {
// 	numDatapoints := 1000
// 	numRanges := 1000
// 	timeInterval := int64(2)
// 	// Create 8 series with 1000 data-points of different ranges, delete and run queries.
// 	lbls := [][]labels.Label{
// 		{
// 			{"a", "b"},
// 			{"instance", "localhost:9090"},
// 			{"job", "prometheus"},
// 		},
// 		{
// 			{"a", "b"},
// 			{"instance", "127.0.0.1:9090"},
// 			{"job", "prometheus"},
// 		},
// 		{
// 			{"a", "b"},
// 			{"instance", "127.0.0.1:9090"},
// 			{"job", "prom-k8s"},
// 		},
// 		{
// 			{"a", "b"},
// 			{"instance", "localhost:9090"},
// 			{"job", "prom-k8s"},
// 		},
// 		{
// 			{"a", "c"},
// 			{"instance", "localhost:9090"},
// 			{"job", "prometheus"},
// 		},
// 		{
// 			{"a", "c"},
// 			{"instance", "127.0.0.1:9090"},
// 			{"job", "prometheus"},
// 		},
// 		{
// 			{"a", "c"},
// 			{"instance", "127.0.0.1:9090"},
// 			{"job", "prom-k8s"},
// 		},
// 		{
// 			{"a", "c"},
// 			{"instance", "localhost:9090"},
// 			{"job", "prom-k8s"},
// 		},
// 	}
// 	seriesMap := map[string][]tsdbutil.Sample{}
// 	for _, l := range lbls {
// 		seriesMap[labels.New(l...).String()] = []tsdbutil.Sample{}
// 	}
// 	dir, _ := ioutil.TempDir("", "test")
// 	defer func() {
// 		testutil.Ok(t, os.RemoveAll(dir))
// 	}()
// 	hb, err := NewHead(nil, nil, nil, 100000)
// 	testutil.Ok(t, err)
// 	defer hb.Close()
// 	app := hb.Appender()
// 	for _, l := range lbls {
// 		ls := labels.New(l...)
// 		series := []tsdbutil.Sample{}
// 		ts := rand.Int63n(300)
// 		for i := 0; i < numDatapoints; i++ {
// 			v := rand.Float64()
// 			_, err := app.Add(ls, ts, v)
// 			testutil.Ok(t, err)
// 			series = append(series, sample{ts, v})
// 			ts += rand.Int63n(timeInterval) + 1
// 		}
// 		seriesMap[labels.New(l...).String()] = series
// 	}
// 	testutil.Ok(t, app.Commit())
// 	// Delete a time-range from each-selector.
// 	dels := []struct {
// 		ms     []labels.Matcher
// 		drange Intervals
// 	}{
// 		{
// 			ms:     []labels.Matcher{labels.NewEqualMatcher("a", "b")},
// 			drange: Intervals{{300, 500}, {600, 670}},
// 		},
// 		{
// 			ms: []labels.Matcher{
// 				labels.NewEqualMatcher("a", "b"),
// 				labels.NewEqualMatcher("job", "prom-k8s"),
// 			},
// 			drange: Intervals{{300, 500}, {100, 670}},
// 		},
// 		{
// 			ms: []labels.Matcher{
// 				labels.NewEqualMatcher("a", "c"),
// 				labels.NewEqualMatcher("instance", "localhost:9090"),
// 				labels.NewEqualMatcher("job", "prometheus"),
// 			},
// 			drange: Intervals{{300, 400}, {100, 6700}},
// 		},
// 		// TODO: Add Regexp Matchers.
// 	}
// 	for _, del := range dels {
// 		for _, r := range del.drange {
// 			testutil.Ok(t, hb.Delete(r.Mint, r.Maxt, del.ms...))
// 		}
// 		matched := labels.Slice{}
// 		for _, ls := range lbls {
// 			s := labels.Selector(del.ms)
// 			if s.Matches(ls) {
// 				matched = append(matched, ls)
// 			}
// 		}
// 		sort.Sort(matched)
// 		for i := 0; i < numRanges; i++ {
// 			q, err := NewBlockQuerier(hb, 0, 100000)
// 			testutil.Ok(t, err)
// 			defer q.Close()
// 			ss, err := q.Select(del.ms...)
// 			testutil.Ok(t, err)
// 			// Build the mockSeriesSet.
// 			matchedSeries := make([]Series, 0, len(matched))
// 			for _, m := range matched {
// 				smpls := seriesMap[m.String()]
// 				smpls = deletedSamples(smpls, del.drange)
// 				// Only append those series for which samples exist as mockSeriesSet
// 				// doesn't skip series with no samples.
// 				// TODO: But sometimes SeriesSet returns an empty SeriesIterator
// 				if len(smpls) > 0 {
// 					matchedSeries = append(matchedSeries, newSeries(
// 						m.Map(),
// 						smpls,
// 					))
// 				}
// 			}
// 			expSs := newMockSeriesSet(matchedSeries)
// 			// Compare both SeriesSets.
// 			for {
// 				eok, rok := expSs.Next(), ss.Next()
// 				// Skip a series if iterator is empty.
// 				if rok {
// 					for !ss.At().Iterator().Next() {
// 						rok = ss.Next()
// 						if !rok {
// 							break
// 						}
// 					}
// 				}
// 				testutil.Equals(t, eok, rok)
// 				if !eok {
// 					break
// 				}
// 				sexp := expSs.At()
// 				sres := ss.At()
// 				testutil.Equals(t, sexp.Labels(), sres.Labels())
// 				smplExp, errExp := expandSeriesIterator(sexp.Iterator())
// 				smplRes, errRes := expandSeriesIterator(sres.Iterator())
// 				testutil.Equals(t, errExp, errRes)
// 				testutil.Equals(t, smplExp, smplRes)
// 			}
// 		}
// 	}
// }

// func boundedSamples(full []tsdbutil.Sample, mint, maxt int64) []tsdbutil.Sample {
// 	for len(full) > 0 {
// 		if full[0].T() >= mint {
// 			break
// 		}
// 		full = full[1:]
// 	}
// 	for i, s := range full {
// 		// labels.Labelinate on the first sample larger than maxt.
// 		if s.T() > maxt {
// 			return full[:i]
// 		}
// 	}
// 	// maxt is after highest sample.
// 	return full
// }

// func deletedSamples(full []tsdbutil.Sample, dranges Intervals) []tsdbutil.Sample {
// 	ds := make([]tsdbutil.Sample, 0, len(full))
// Outer:
// 	for _, s := range full {
// 		for _, r := range dranges {
// 			if r.inBounds(s.T()) {
// 				continue Outer
// 			}
// 		}
// 		ds = append(ds, s)
// 	}

// 	return ds
// }

// func TestComputeChunkEndTime(t *testing.T) {
// 	cases := []struct {
// 		start, cur, max int64
// 		res             int64
// 	}{
// 		{
// 			start: 0,
// 			cur:   250,
// 			max:   1000,
// 			res:   1000,
// 		},
// 		{
// 			start: 100,
// 			cur:   200,
// 			max:   1000,
// 			res:   550,
// 		},
// 		// Case where we fit floored 0 chunks. Must catch division by 0
// 		// and default to maximum time.
// 		{
// 			start: 0,
// 			cur:   500,
// 			max:   1000,
// 			res:   1000,
// 		},
// 		// Catch division by zero for cur == start. Strictly not a possible case.
// 		{
// 			start: 100,
// 			cur:   100,
// 			max:   1000,
// 			res:   104,
// 		},
// 	}

// 	for _, c := range cases {
// 		got := computeChunkEndTime(c.start, c.cur, c.max)
// 		if got != c.res {
// 			t.Errorf("expected %d for (start: %d, cur: %d, max: %d), got %d", c.res, c.start, c.cur, c.max, got)
// 		}
// 	}
// }

// func TestMemSeries_append(t *testing.T) {
// 	s := newMemSeries(labels.Labels{}, 1, 500)

// 	// Add first two samples at the very end of a chunk range and the next two
// 	// on and after it.
// 	// New chunk must correctly be cut at 1000.
// 	ok, chunkCreated := s.append(998, 1)
// 	testutil.Assert(t, ok, "append failed")
// 	testutil.Assert(t, chunkCreated, "first sample created chunk")

// 	ok, chunkCreated = s.append(999, 2)
// 	testutil.Assert(t, ok, "append failed")
// 	testutil.Assert(t, !chunkCreated, "second sample should use same chunk")

// 	ok, chunkCreated = s.append(1000, 3)
// 	testutil.Assert(t, ok, "append failed")
// 	testutil.Assert(t, chunkCreated, "expected new chunk on boundary")

// 	ok, chunkCreated = s.append(1001, 4)
// 	testutil.Assert(t, ok, "append failed")
// 	testutil.Assert(t, !chunkCreated, "second sample should use same chunk")

// 	testutil.Assert(t, s.chunks[0].minTime == 998 && s.chunks[0].maxTime == 999, "wrong chunk range")
// 	testutil.Assert(t, s.chunks[1].minTime == 1000 && s.chunks[1].maxTime == 1001, "wrong chunk range")

// 	// Fill the range [1000,2000) with many samples. Intermediate chunks should be cut
// 	// at approximately 120 samples per chunk.
// 	for i := 1; i < 1000; i++ {
// 		ok, _ := s.append(1001+int64(i), float64(i))
// 		testutil.Assert(t, ok, "append failed")
// 	}

// 	testutil.Assert(t, len(s.chunks) > 7, "expected intermediate chunks")

// 	// All chunks but the first and last should now be moderately full.
// 	for i, c := range s.chunks[1 : len(s.chunks)-1] {
// 		testutil.Assert(t, c.chunk.NumSamples() > 100, "unexpected small chunk %d of length %d", i, c.chunk.NumSamples())
// 	}
// }

// func TestGCChunkAccess(t *testing.T) {
// 	// Put a chunk, select it. GC it and then access it.
// 	h, err := NewHead(nil, nil, nil, 1000)
// 	testutil.Ok(t, err)
// 	defer h.Close()

// 	h.initTime(0)

// 	s, _ := h.getOrCreate(1, labels.FromStrings("a", "1"))
// 	s.chunks = []*memChunk{
// 		{minTime: 0, maxTime: 999},
// 		{minTime: 1000, maxTime: 1999},
// 	}

// 	idx := h.indexRange(0, 1500)
// 	var (
// 		lset   labels.Labels
// 		chunks []chunks.Meta
// 	)
// 	testutil.Ok(t, idx.Series(1, &lset, &chunks))

// 	testutil.Equals(t, labels.Labels{{
// 		Name: "a", Value: "1",
// 	}}, lset)
// 	testutil.Equals(t, 2, len(chunks))

// 	cr := h.chunksRange(0, 1500)
// 	_, err = cr.Chunk(chunks[0].Ref)
// 	testutil.Ok(t, err)
// 	_, err = cr.Chunk(chunks[1].Ref)
// 	testutil.Ok(t, err)

// 	testutil.Ok(t, h.Truncate(1500)) // Remove a chunk.

// 	_, err = cr.Chunk(chunks[0].Ref)
// 	testutil.Equals(t, ErrNotFound, err)
// 	_, err = cr.Chunk(chunks[1].Ref)
// 	testutil.Ok(t, err)
// }

// func TestGCSeriesAccess(t *testing.T) {
// 	// Put a series, select it. GC it and then access it.
// 	h, err := NewHead(nil, nil, nil, 1000)
// 	testutil.Ok(t, err)
// 	defer h.Close()

// 	h.initTime(0)

// 	s, _ := h.getOrCreate(1, labels.FromStrings("a", "1"))
// 	s.chunks = []*memChunk{
// 		{minTime: 0, maxTime: 999},
// 		{minTime: 1000, maxTime: 1999},
// 	}

// 	idx := h.indexRange(0, 2000)
// 	var (
// 		lset   labels.Labels
// 		chunks []chunks.Meta
// 	)
// 	testutil.Ok(t, idx.Series(1, &lset, &chunks))

// 	testutil.Equals(t, labels.Labels{{
// 		Name: "a", Value: "1",
// 	}}, lset)
// 	testutil.Equals(t, 2, len(chunks))

// 	cr := h.chunksRange(0, 2000)
// 	_, err = cr.Chunk(chunks[0].Ref)
// 	testutil.Ok(t, err)
// 	_, err = cr.Chunk(chunks[1].Ref)
// 	testutil.Ok(t, err)

// 	testutil.Ok(t, h.Truncate(2000)) // Remove the series.

// 	testutil.Equals(t, (*memSeries)(nil), h.series.getByID(1))

// 	_, err = cr.Chunk(chunks[0].Ref)
// 	testutil.Equals(t, ErrNotFound, err)
// 	_, err = cr.Chunk(chunks[1].Ref)
// 	testutil.Equals(t, ErrNotFound, err)
// }

// func TestUncommittedSamplesNotLostOnTruncate(t *testing.T) {
// 	h, err := NewHead(nil, nil, nil, 1000)
// 	testutil.Ok(t, err)
// 	defer h.Close()

// 	h.initTime(0)

// 	app := h.appender()
// 	lset := labels.FromStrings("a", "1")
// 	_, err = app.Add(lset, 2100, 1)
// 	testutil.Ok(t, err)

// 	testutil.Ok(t, h.Truncate(2000))
// 	testutil.Assert(t, nil != h.series.getByHash(lset.Hash(), lset), "series should not have been garbage collected")

// 	testutil.Ok(t, app.Commit())

// 	q, err := NewBlockQuerier(h, 1500, 2500)
// 	testutil.Ok(t, err)
// 	defer q.Close()

// 	ss, err := q.Select(labels.NewEqualMatcher("a", "1"))
// 	testutil.Ok(t, err)

// 	testutil.Equals(t, true, ss.Next())
// }

// func TestRemoveSeriesAfterRollbackAndTruncate(t *testing.T) {
// 	h, err := NewHead(nil, nil, nil, 1000)
// 	testutil.Ok(t, err)
// 	defer h.Close()

// 	h.initTime(0)

// 	app := h.appender()
// 	lset := labels.FromStrings("a", "1")
// 	_, err = app.Add(lset, 2100, 1)
// 	testutil.Ok(t, err)

// 	testutil.Ok(t, h.Truncate(2000))
// 	testutil.Assert(t, nil != h.series.getByHash(lset.Hash(), lset), "series should not have been garbage collected")

// 	testutil.Ok(t, app.Rollback())

// 	q, err := NewBlockQuerier(h, 1500, 2500)
// 	testutil.Ok(t, err)
// 	defer q.Close()

// 	ss, err := q.Select(labels.NewEqualMatcher("a", "1"))
// 	testutil.Ok(t, err)

// 	testutil.Equals(t, false, ss.Next())

// 	// Truncate again, this time the series should be deleted
// 	testutil.Ok(t, h.Truncate(2050))
// 	testutil.Equals(t, (*memSeries)(nil), h.series.getByHash(lset.Hash(), lset))
// }

// func TestHead_LogRollback(t *testing.T) {
// 	for _, compress := range []bool{false, true} {
// 		t.Run(fmt.Sprintf("compress=%t", compress), func(t *testing.T) {
// 			dir, err := ioutil.TempDir("", "wal_rollback")
// 			testutil.Ok(t, err)
// 			defer func() {
// 				testutil.Ok(t, os.RemoveAll(dir))
// 			}()

// 			w, err := wal.New(nil, nil, dir, compress)
// 			testutil.Ok(t, err)
// 			defer w.Close()
// 			h, err := NewHead(nil, nil, w, 1000)
// 			testutil.Ok(t, err)

// 			app := h.Appender()
// 			_, err = app.Add(labels.FromStrings("a", "b"), 1, 2)
// 			testutil.Ok(t, err)

// 			testutil.Ok(t, app.Rollback())
// 			recs := readTestWAL(t, w.Dir())

// 			testutil.Equals(t, 1, len(recs))

// 			series, ok := recs[0].([]RefSeries)
// 			testutil.Assert(t, ok, "expected series record but got %+v", recs[0])
// 			testutil.Equals(t, []RefSeries{{Ref: 1, Labels: labels.FromStrings("a", "b")}}, series)
// 		})
// 	}
// }

// // TestWalRepair_DecodingError ensures that a repair is run for an error
// // when decoding a record.
// func TestWalRepair_DecodingError(t *testing.T) {
// 	var enc RecordEncoder
// 	for name, test := range map[string]struct {
// 		corrFunc  func(rec []byte) []byte // Func that applies the corruption to a record.
// 		rec       []byte
// 		totalRecs int
// 		expRecs   int
// 	}{
// 		"invalid_record": {
// 			func(rec []byte) []byte {
// 				// Do not modify the base record because it is Logged multiple times.
// 				res := make([]byte, len(rec))
// 				copy(res, rec)
// 				res[0] = byte(RecordInvalid)
// 				return res
// 			},
// 			enc.Series([]RefSeries{{Ref: 1, Labels: labels.FromStrings("a", "b")}}, []byte{}),
// 			9,
// 			5,
// 		},
// 		"decode_series": {
// 			func(rec []byte) []byte {
// 				return rec[:3]
// 			},
// 			enc.Series([]RefSeries{{Ref: 1, Labels: labels.FromStrings("a", "b")}}, []byte{}),
// 			9,
// 			5,
// 		},
// 		"decode_samples": {
// 			func(rec []byte) []byte {
// 				return rec[:3]
// 			},
// 			enc.Samples([]RefSample{{Ref: 0, T: 99, V: 1}}, []byte{}),
// 			9,
// 			5,
// 		},
// 		"decode_tombstone": {
// 			func(rec []byte) []byte {
// 				return rec[:3]
// 			},
// 			enc.Tombstones([]Stone{{ref: 1, intervals: Intervals{}}}, []byte{}),
// 			9,
// 			5,
// 		},
// 	} {
// 		for _, compress := range []bool{false, true} {
// 			t.Run(fmt.Sprintf("%s,compress=%t", name, compress), func(t *testing.T) {
// 				dir, err := ioutil.TempDir("", "wal_repair")
// 				testutil.Ok(t, err)
// 				defer func() {
// 					testutil.Ok(t, os.RemoveAll(dir))
// 				}()

// 				// Fill the wal and corrupt it.
// 				{
// 					w, err := wal.New(nil, nil, filepath.Join(dir, "wal"), compress)
// 					testutil.Ok(t, err)

// 					for i := 1; i <= test.totalRecs; i++ {
// 						// At this point insert a corrupted record.
// 						if i-1 == test.expRecs {
// 							testutil.Ok(t, w.Log(test.corrFunc(test.rec)))
// 							continue
// 						}
// 						testutil.Ok(t, w.Log(test.rec))
// 					}

// 					h, err := NewHead(nil, nil, w, 1)
// 					testutil.Ok(t, err)
// 					testutil.Equals(t, 0.0, prom_testutil.ToFloat64(h.metrics.walCorruptionsTotal))
// 					initErr := h.Init(math.MinInt64)

// 					err = errors.Cause(initErr) // So that we can pick up errors even if wrapped.
// 					_, corrErr := err.(*wal.CorruptionErr)
// 					testutil.Assert(t, corrErr, "reading the wal didn't return corruption error")
// 					testutil.Ok(t, w.Close())
// 				}

// 				// Open the db to trigger a repair.
// 				{
// 					db, err := Open(dir, nil, nil, DefaultOptions)
// 					testutil.Ok(t, err)
// 					defer func() {
// 						testutil.Ok(t, db.Close())
// 					}()
// 					testutil.Equals(t, 1.0, prom_testutil.ToFloat64(db.head.metrics.walCorruptionsTotal))
// 				}

// 				// Read the wal content after the repair.
// 				{
// 					sr, err := wal.NewSegmentsReader(filepath.Join(dir, "wal"))
// 					testutil.Ok(t, err)
// 					defer sr.Close()
// 					r := wal.NewReader(sr)

// 					var actRec int
// 					for r.Next() {
// 						actRec++
// 					}
// 					testutil.Ok(t, r.Err())
// 					testutil.Equals(t, test.expRecs, actRec, "Wrong number of intact records")
// 				}
// 			})
// 		}
// 	}
// }

// func TestNewWalSegmentOnTruncate(t *testing.T) {
// 	dir, err := ioutil.TempDir("", "test_wal_segemnts")
// 	testutil.Ok(t, err)
// 	defer func() {
// 		testutil.Ok(t, os.RemoveAll(dir))
// 	}()
// 	wlog, err := wal.NewSize(nil, nil, dir, 32768, false)
// 	testutil.Ok(t, err)

// 	h, err := NewHead(nil, nil, wlog, 1000)
// 	testutil.Ok(t, err)
// 	defer h.Close()
// 	add := func(ts int64) {
// 		app := h.Appender()
// 		_, err := app.Add(labels.Labels{{"a", "b"}}, ts, 0)
// 		testutil.Ok(t, err)
// 		testutil.Ok(t, app.Commit())
// 	}

// 	add(0)
// 	_, last, err := wlog.Segments()
// 	testutil.Ok(t, err)
// 	testutil.Equals(t, 0, last)

// 	add(1)
// 	testutil.Ok(t, h.Truncate(1))
// 	_, last, err = wlog.Segments()
// 	testutil.Ok(t, err)
// 	testutil.Equals(t, 1, last)

// 	add(2)
// 	testutil.Ok(t, h.Truncate(2))
// 	_, last, err = wlog.Segments()
// 	testutil.Ok(t, err)
// 	testutil.Equals(t, 2, last)
// }
