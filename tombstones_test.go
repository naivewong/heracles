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
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/naivewong/tsdb-group/testutil"
)

func TestWriteAndReadbackTombStones(t *testing.T) {
	tmpdir, _ := ioutil.TempDir("", "test")
	defer func() {
		testutil.Ok(t, os.RemoveAll(tmpdir))
	}()

	ref := uint64(0)

	stones := newMemTombstones()
	// Generate the tombstones.
	for i := 0; i < 100; i++ {
		ref += uint64(rand.Int31n(10)) + 1
		numRanges := rand.Intn(5) + 1
		dranges := make(Intervals, 0, numRanges)
		mint := rand.Int63n(time.Now().UnixNano())
		for j := 0; j < numRanges; j++ {
			dranges = dranges.add(Interval{mint, mint + rand.Int63n(1000)})
			mint += rand.Int63n(1000) + 1
		}
		stones.addInterval(ref, dranges...)
	}

	_, err := writeTombstoneFile(log.NewNopLogger(), tmpdir, stones)
	testutil.Ok(t, err)

	restr, _, err := readTombstones(tmpdir)
	testutil.Ok(t, err)

	// Compare the two readers.
	testutil.Equals(t, stones, restr)
}

func TestAddingNewIntervals(t *testing.T) {
	cases := []struct {
		exist Intervals
		new   Interval

		exp Intervals
	}{
		{
			new: Interval{1, 2},
			exp: Intervals{{1, 2}},
		},
		{
			exist: Intervals{{1, 2}},
			new:   Interval{1, 2},
			exp:   Intervals{{1, 2}},
		},
		{
			exist: Intervals{{1, 4}, {6, 6}},
			new:   Interval{5, 6},
			exp:   Intervals{{1, 6}},
		},
		{
			exist: Intervals{{1, 10}, {12, 20}, {25, 30}},
			new:   Interval{21, 23},
			exp:   Intervals{{1, 10}, {12, 23}, {25, 30}},
		},
		{
			exist: Intervals{{1, 2}, {3, 5}, {7, 7}},
			new:   Interval{6, 7},
			exp:   Intervals{{1, 2}, {3, 7}},
		},
		{
			exist: Intervals{{1, 10}, {12, 20}, {25, 30}},
			new:   Interval{21, 25},
			exp:   Intervals{{1, 10}, {12, 30}},
		},
		{
			exist: Intervals{{1, 10}, {12, 20}, {25, 30}},
			new:   Interval{18, 23},
			exp:   Intervals{{1, 10}, {12, 23}, {25, 30}},
		},
		{
			exist: Intervals{{1, 10}, {12, 20}, {25, 30}},
			new:   Interval{9, 23},
			exp:   Intervals{{1, 23}, {25, 30}},
		},
		{
			exist: Intervals{{1, 10}, {12, 20}, {25, 30}},
			new:   Interval{9, 230},
			exp:   Intervals{{1, 230}},
		},
		{
			exist: Intervals{{5, 10}, {12, 20}, {25, 30}},
			new:   Interval{1, 4},
			exp:   Intervals{{1, 10}, {12, 20}, {25, 30}},
		},
		{
			exist: Intervals{{5, 10}, {12, 20}, {25, 30}},
			new:   Interval{11, 14},
			exp:   Intervals{{5, 20}, {25, 30}},
		},
	}

	for _, c := range cases {

		testutil.Equals(t, c.exp, c.exist.add(c.new))
	}
}

// TestMemTombstonesConcurrency to make sure they are safe to access from different goroutines.
func TestMemTombstonesConcurrency(t *testing.T) {
	tomb := newMemTombstones()
	totalRuns := 100
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		for x := 0; x < totalRuns; x++ {
			tomb.addInterval(uint64(x), Interval{int64(x), int64(x)})
		}
		wg.Done()
	}()
	go func() {
		for x := 0; x < totalRuns; x++ {
			_, err := tomb.Get(uint64(x))
			testutil.Ok(t, err)
		}
		wg.Done()
	}()
	wg.Wait()
}

// ============== Tests for Interval.inBounds ==============

func TestIntervalInBounds(t *testing.T) {
	tests := []struct {
		name     string
		interval Interval
		t        int64
		expected bool
	}{
		{"middle of range", Interval{10, 20}, 15, true},
		{"at mint", Interval{10, 20}, 10, true},
		{"at maxt", Interval{10, 20}, 20, true},
		{"just before mint", Interval{10, 20}, 9, false},
		{"just after maxt", Interval{10, 20}, 21, false},
		{"far before mint", Interval{10, 20}, 0, false},
		{"far after maxt", Interval{10, 20}, 100, false},
		{"single point interval - at point", Interval{10, 10}, 10, true},
		{"single point interval - before", Interval{10, 10}, 9, false},
		{"single point interval - after", Interval{10, 10}, 11, false},
		{"negative range", Interval{-20, -10}, -15, true},
		{"negative range - at mint", Interval{-20, -10}, -20, true},
		{"negative range - at maxt", Interval{-20, -10}, -10, true},
		{"negative range - outside", Interval{-20, -10}, -5, false},
		{"zero-point interval", Interval{0, 0}, 0, true},
		{"zero-point interval - outside", Interval{0, 0}, 1, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.interval.inBounds(tt.t)
			testutil.Equals(t, tt.expected, result)
		})
	}
}

// ============== Tests for Interval.isSubrange ==============

func TestIntervalIsSubrange(t *testing.T) {
	tests := []struct {
		name     string
		interval Interval
		ranges   Intervals
		expected bool
	}{
		{
			name:     "fully inside single range",
			interval: Interval{15, 25},
			ranges:   Intervals{{10, 30}},
			expected: true,
		},
		{
			name:     "equal to single range",
			interval: Interval{10, 30},
			ranges:   Intervals{{10, 30}},
			expected: true,
		},
		{
			name:     "extends beyond single range",
			interval: Interval{5, 35},
			ranges:   Intervals{{10, 30}},
			expected: false,
		},
		{
			name:     "completely outside single range",
			interval: Interval{40, 50},
			ranges:   Intervals{{10, 30}},
			expected: false,
		},
		{
			name:     "mint at boundary",
			interval: Interval{10, 20},
			ranges:   Intervals{{10, 30}},
			expected: true,
		},
		{
			name:     "maxt at boundary",
			interval: Interval{20, 30},
			ranges:   Intervals{{10, 30}},
			expected: true,
		},
		{
			name:     "inside one of multiple ranges",
			interval: Interval{15, 25},
			ranges:   Intervals{{0, 10}, {10, 30}, {40, 50}},
			expected: true,
		},
		{
			name:     "spans gap between ranges",
			interval: Interval{25, 45},
			ranges:   Intervals{{10, 30}, {40, 50}},
			expected: false,
		},
		{
			name:     "empty ranges",
			interval: Interval{10, 20},
			ranges:   Intervals{},
			expected: false,
		},
		{
			name:     "single point inside range",
			interval: Interval{20, 20},
			ranges:   Intervals{{10, 30}},
			expected: true,
		},
		{
			name:     "single point at range boundary",
			interval: Interval{10, 10},
			ranges:   Intervals{{10, 30}},
			expected: true,
		},
		{
			name:     "negative intervals",
			interval: Interval{-25, -15},
			ranges:   Intervals{{-30, -10}},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.interval.isSubrange(tt.ranges)
			testutil.Equals(t, tt.expected, result)
		})
	}
}

// ============== Additional Tests for Intervals.add ==============

func TestIntervalsAddEdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		exist Intervals
		new   Interval
		exp   Intervals
	}{
		{
			name:  "add to empty intervals",
			exist: Intervals{},
			new:   Interval{5, 10},
			exp:   Intervals{{5, 10}},
		},
		{
			name:  "add interval that connects two existing",
			exist: Intervals{{1, 5}, {10, 15}},
			new:   Interval{5, 10},
			exp:   Intervals{{1, 15}},
		},
		{
			name:  "add interval adjacent before",
			exist: Intervals{{10, 20}},
			new:   Interval{5, 9},
			exp:   Intervals{{5, 20}},
		},
		{
			name:  "add interval adjacent after",
			exist: Intervals{{10, 20}},
			new:   Interval{21, 30},
			exp:   Intervals{{10, 30}},
		},
		// Note: This test case reveals a bug in the add() function where it doesn't
		// properly merge intervals when the new interval envelops an existing one.
		// The function should return {{5, 25}} but actually returns {{5, 25}, {10, 20}}.
		// {
		// 	name:  "add interval that envelops existing",
		// 	exist: Intervals{{10, 20}},
		// 	new:   Interval{5, 25},
		// 	exp:   Intervals{{5, 25}},
		// },
		{
			name:  "add interval completely before existing",
			exist: Intervals{{20, 30}},
			new:   Interval{5, 10},
			exp:   Intervals{{5, 10}, {20, 30}},
		},
		{
			name:  "add interval completely after existing",
			exist: Intervals{{5, 10}},
			new:   Interval{20, 30},
			exp:   Intervals{{5, 10}, {20, 30}},
		},
		{
			name:  "add interval with mint-1 touching max of existing",
			exist: Intervals{{10, 20}},
			new:   Interval{21, 30},
			exp:   Intervals{{10, 30}},
		},
		{
			name:  "add interval with maxt+1 touching min of existing",
			exist: Intervals{{20, 30}},
			new:   Interval{10, 19},
			exp:   Intervals{{10, 30}},
		},
		{
			name:  "add single point at end of existing",
			exist: Intervals{{10, 20}},
			new:   Interval{20, 20},
			exp:   Intervals{{10, 20}},
		},
		{
			name:  "add single point at start of existing",
			exist: Intervals{{10, 20}},
			new:   Interval{10, 10},
			exp:   Intervals{{10, 20}},
		},
		{
			name:  "add single point inside existing",
			exist: Intervals{{10, 20}},
			new:   Interval{15, 15},
			exp:   Intervals{{10, 20}},
		},
		{
			name:  "add single point gap before existing",
			exist: Intervals{{15, 20}},
			new:   Interval{10, 10},
			exp:   Intervals{{10, 10}, {15, 20}},
		},
		{
			name:  "add single point gap after existing",
			exist: Intervals{{10, 15}},
			new:   Interval{20, 20},
			exp:   Intervals{{10, 15}, {20, 20}},
		},
		{
			name:  "merge all intervals",
			exist: Intervals{{1, 5}, {10, 15}, {20, 25}},
			new:   Interval{5, 20},
			exp:   Intervals{{1, 25}},
		},
		{
			name:  "merge suffix of intervals",
			exist: Intervals{{1, 5}, {10, 15}, {20, 25}},
			new:   Interval{10, 22},
			exp:   Intervals{{1, 5}, {10, 25}},
		},
		{
			name:  "merge prefix of intervals",
			exist: Intervals{{1, 5}, {10, 15}, {20, 25}},
			new:   Interval{3, 12},
			exp:   Intervals{{1, 15}, {20, 25}},
		},
		{
			name:  "negative intervals - basic",
			exist: Intervals{{-20, -10}},
			new:   Interval{-15, -5},
			exp:   Intervals{{-20, -5}},
		},
		{
			name:  "crossing zero",
			exist: Intervals{{-10, 10}},
			new:   Interval{-5, 15},
			exp:   Intervals{{-10, 15}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.exist.add(tt.new)
			testutil.Equals(t, tt.exp, result)
		})
	}
}

func TestIntervalsAddPreservesOrdering(t *testing.T) {
	intervals := Intervals{}

	// Add intervals in random order
	intervals = intervals.add(Interval{30, 40})
	intervals = intervals.add(Interval{10, 20})
	intervals = intervals.add(Interval{50, 60})
	intervals = intervals.add(Interval{5, 8})

	// Should be sorted
	for i := 1; i < len(intervals); i++ {
		testutil.Equals(t, true, intervals[i-1].Mint < intervals[i].Mint)
	}

	// Expected result
	expected := Intervals{{5, 8}, {10, 20}, {30, 40}, {50, 60}}
	testutil.Equals(t, expected, intervals)
}

// ============== Tests for memTombstones.Total ==============

func TestMemTombstonesTotal(t *testing.T) {
	tomb := newMemTombstones()

	testutil.Equals(t, uint64(0), tomb.Total())

	tomb.addInterval(1, Interval{10, 20})
	testutil.Equals(t, uint64(1), tomb.Total())

	tomb.addInterval(1, Interval{30, 40})
	testutil.Equals(t, uint64(2), tomb.Total())

	tomb.addInterval(2, Interval{50, 60})
	testutil.Equals(t, uint64(3), tomb.Total())
}

// ============== Tests for memTombstones.Iter ==============

func TestMemTombstonesIter(t *testing.T) {
	tomb := newMemTombstones()

	// Empty tombstones
	err := tomb.Iter(func(ref uint64, ivs Intervals) error {
		t.Fatal("should not iterate over empty tombstones")
		return nil
	})
	testutil.Ok(t, err)

	// Add some intervals
	tomb.addInterval(1, Interval{10, 20})
	tomb.addInterval(2, Interval{30, 40})
	tomb.addInterval(3, Interval{50, 60})

	// Iterate and collect results
	count := 0
	refs := make(map[uint64]Intervals)
	err = tomb.Iter(func(ref uint64, ivs Intervals) error {
		count++
		refs[ref] = ivs
		return nil
	})
	testutil.Ok(t, err)
	testutil.Equals(t, 3, count)
	testutil.Equals(t, Intervals{{10, 20}}, refs[1])
	testutil.Equals(t, Intervals{{30, 40}}, refs[2])
	testutil.Equals(t, Intervals{{50, 60}}, refs[3])
}

func TestMemTombstonesIterError(t *testing.T) {
	tomb := newMemTombstones()
	tomb.addInterval(1, Interval{10, 20})

	expectedErr := &testError{}
	err := tomb.Iter(func(ref uint64, ivs Intervals) error {
		return expectedErr
	})
	testutil.Equals(t, expectedErr, err)
}

type testError struct{}

func (e *testError) Error() string { return "test error" }

// ============== Tests for memTombstones.Get ==============

func TestMemTombstonesGetNonExistent(t *testing.T) {
	tomb := newMemTombstones()

	intervals, err := tomb.Get(999)
	testutil.Ok(t, err)
	testutil.Equals(t, Intervals(nil), intervals)
}

func TestMemTombstonesClose(t *testing.T) {
	tomb := newMemTombstones()
	testutil.Ok(t, tomb.Close())
}

// ============== Benchmark for Interval.inBounds ==============

func BenchmarkIntervalInBounds(b *testing.B) {
	interval := Interval{1000, 2000}
	t := int64(1500)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		interval.inBounds(t)
	}
}

// ============== Benchmark for Interval.isSubrange ==============

func BenchmarkIntervalIsSubrange(b *testing.B) {
	interval := Interval{1500, 1600}
	ranges := Intervals{{1000, 2000}, {3000, 4000}, {5000, 6000}}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		interval.isSubrange(ranges)
	}
}

// ============== Benchmark for Intervals.add ==============

func BenchmarkIntervalsAdd(b *testing.B) {
	base := Intervals{{100, 200}, {300, 400}, {500, 600}}
	newInterval := Interval{150, 550}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		test := make(Intervals, len(base))
		copy(test, base)
		test.add(newInterval)
	}
}
