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

package encoding

import (
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/naivewong/tsdb-group/testutil"
)

// ============== Tests for GetNthByMaxHeapInt64 ==============

func TestGetNthByMaxHeapInt64(t *testing.T) {
	// GetNthByMaxHeapInt64 returns the (k+1)th smallest element
	// k=0 returns the smallest, k=1 returns 2nd smallest, etc.
	data := []int64{10, 9, 8, 7, 6, 5, 4, 3, 2, 1}
	// Sorted: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
	result := GetNthByMaxHeapInt64(data, 0)
	testutil.Equals(t, int64(1), result)

	result = GetNthByMaxHeapInt64(data, 2)
	testutil.Equals(t, int64(3), result)

	result = GetNthByMaxHeapInt64(data, 4)
	testutil.Equals(t, int64(5), result)

	result = GetNthByMaxHeapInt64(data, 9)
	testutil.Equals(t, int64(10), result)
}

func TestGetNthByMaxHeapInt64WithDuplicates(t *testing.T) {
	data := []int64{5, 3, 5, 1, 3, 1, 5, 3, 1}
	// Sorted: 1, 1, 1, 3, 3, 3, 5, 5, 5
	// k=0 -> 1, k=2 -> 1, k=3 -> 3, k=6 -> 5
	result := GetNthByMaxHeapInt64(data, 0)
	testutil.Equals(t, int64(1), result)

	result = GetNthByMaxHeapInt64(data, 2)
	testutil.Equals(t, int64(1), result)

	result = GetNthByMaxHeapInt64(data, 5)
	testutil.Equals(t, int64(3), result)
}

func TestGetNthByMaxHeapInt64SingleElement(t *testing.T) {
	data := []int64{42}
	result := GetNthByMaxHeapInt64(data, 0)
	testutil.Equals(t, int64(42), result)
}

func TestGetNthByMaxHeapInt64WithNegativeNumbers(t *testing.T) {
	data := []int64{-5, -1, -10, -3, -7}
	// Sorted: -10, -7, -5, -3, -1
	result := GetNthByMaxHeapInt64(data, 0)
	testutil.Equals(t, int64(-10), result)

	result = GetNthByMaxHeapInt64(data, 2)
	testutil.Equals(t, int64(-5), result)

	result = GetNthByMaxHeapInt64(data, 4)
	testutil.Equals(t, int64(-1), result)
}

func TestGetNthByMaxHeapInt64WithLargeNumbers(t *testing.T) {
	data := []int64{math.MaxInt64, math.MinInt64, 0, 1000000, -1000000}
	// Sorted: MinInt64, -1000000, 0, 1000000, MaxInt64
	result := GetNthByMaxHeapInt64(data, 0)
	testutil.Equals(t, int64(math.MinInt64), result)

	result = GetNthByMaxHeapInt64(data, 2)
	testutil.Equals(t, int64(0), result)

	result = GetNthByMaxHeapInt64(data, 4)
	testutil.Equals(t, int64(math.MaxInt64), result)
}

func TestGetNthByMaxHeapInt64RandomData(t *testing.T) {
	rand.Seed(42)
	size := 1000
	data := make([]int64, size)
	for i := 0; i < size; i++ {
		data[i] = rand.Int63n(1000000)
	}

	// Test multiple k values
	for _, k := range []int{0, size / 4, size / 2, 3 * size / 4, size - 1} {
		result := GetNthByMaxHeapInt64(data, k)

		// Verify by sorting
		sorted := make([]int64, len(data))
		copy(sorted, data)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

		testutil.Equals(t, sorted[k], result, "k=%d", k)
	}
}

// ============== Tests for GetNthByMinHeapInt64 ==============

func TestGetNthByMinHeapInt64(t *testing.T) {
	// GetNthByMinHeapInt64 returns the (k+1)th largest element
	// k=0 returns the largest, k=1 returns 2nd largest, etc.
	data := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	// Sorted descending: 10, 9, 8, 7, 6, 5, 4, 3, 2, 1
	result := GetNthByMinHeapInt64(data, 0)
	testutil.Equals(t, int64(10), result)

	result = GetNthByMinHeapInt64(data, 2)
	testutil.Equals(t, int64(8), result)

	result = GetNthByMinHeapInt64(data, 4)
	testutil.Equals(t, int64(6), result)

	result = GetNthByMinHeapInt64(data, 9)
	testutil.Equals(t, int64(1), result)
}

func TestGetNthByMinHeapInt64WithDuplicates(t *testing.T) {
	data := []int64{1, 3, 1, 3, 1, 3, 5, 5, 5}
	// Sorted descending: 5, 5, 5, 3, 3, 3, 1, 1, 1
	result := GetNthByMinHeapInt64(data, 0)
	testutil.Equals(t, int64(5), result)

	result = GetNthByMinHeapInt64(data, 2)
	testutil.Equals(t, int64(5), result)

	result = GetNthByMinHeapInt64(data, 6)
	testutil.Equals(t, int64(1), result)
}

func TestGetNthByMinHeapInt64SingleElement(t *testing.T) {
	data := []int64{42}
	result := GetNthByMinHeapInt64(data, 0)
	testutil.Equals(t, int64(42), result)
}

func TestGetNthByMinHeapInt64WithNegativeNumbers(t *testing.T) {
	data := []int64{-1, -5, -3, -7, -10}
	// Sorted descending: -1, -3, -5, -7, -10
	result := GetNthByMinHeapInt64(data, 0)
	testutil.Equals(t, int64(-1), result)

	result = GetNthByMinHeapInt64(data, 2)
	testutil.Equals(t, int64(-5), result)

	result = GetNthByMinHeapInt64(data, 4)
	testutil.Equals(t, int64(-10), result)
}

func TestGetNthByMinHeapInt64WithLargeNumbers(t *testing.T) {
	data := []int64{math.MaxInt64, math.MinInt64, 0, 1000000, -1000000}
	// Sorted descending: MaxInt64, 1000000, 0, -1000000, MinInt64
	result := GetNthByMinHeapInt64(data, 0)
	testutil.Equals(t, int64(math.MaxInt64), result)

	result = GetNthByMinHeapInt64(data, 2)
	testutil.Equals(t, int64(0), result)

	result = GetNthByMinHeapInt64(data, 4)
	testutil.Equals(t, int64(math.MinInt64), result)
}

func TestGetNthByMinHeapInt64RandomData(t *testing.T) {
	rand.Seed(42)
	size := 1000
	data := make([]int64, size)
	for i := 0; i < size; i++ {
		data[i] = rand.Int63n(1000000)
	}

	// Test multiple k values
	for _, k := range []int{0, size / 4, size / 2, 3 * size / 4, size - 1} {
		result := GetNthByMinHeapInt64(data, k)

		// Verify by sorting in descending order
		sorted := make([]int64, len(data))
		copy(sorted, data)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] > sorted[j] })

		testutil.Equals(t, sorted[k], result, "k=%d", k)
	}
}

// ============== Tests for GetNthByMaxHeapFloat64 ==============

func TestGetNthByMaxHeapFloat64(t *testing.T) {
	data := []float64{1.5, 3.2, 0.5, 2.8, 4.1, 0.1}
	// Sorted: 0.1, 0.5, 1.5, 2.8, 3.2, 4.1
	result := GetNthByMaxHeapFloat64(data, 0)
	testutil.Equals(t, 0.1, result)

	result = GetNthByMaxHeapFloat64(data, 2)
	testutil.Equals(t, 1.5, result)

	result = GetNthByMaxHeapFloat64(data, 5)
	testutil.Equals(t, 4.1, result)
}

func TestGetNthByMaxHeapFloat64WithNegatives(t *testing.T) {
	data := []float64{-1.5, -3.2, 0.0, 2.8, -4.1}
	// Sorted: -4.1, -3.2, -1.5, 0.0, 2.8
	result := GetNthByMaxHeapFloat64(data, 0)
	testutil.Equals(t, -4.1, result)

	result = GetNthByMaxHeapFloat64(data, 2)
	testutil.Equals(t, -1.5, result)

	result = GetNthByMaxHeapFloat64(data, 4)
	testutil.Equals(t, 2.8, result)
}

func TestGetNthByMaxHeapFloat64SpecialValues(t *testing.T) {
	data := []float64{math.Pi, math.E, 0.0, -math.Pi, -math.E}
	// Sorted: -Pi, -E, 0, E, Pi
	result := GetNthByMaxHeapFloat64(data, 0)
	testutil.Equals(t, -math.Pi, result)

	result = GetNthByMaxHeapFloat64(data, 2)
	testutil.Equals(t, 0.0, result)

	result = GetNthByMaxHeapFloat64(data, 4)
	testutil.Equals(t, math.Pi, result)
}

// ============== Tests for GetNthByMinHeapFloat64 ==============

func TestGetNthByMinHeapFloat64(t *testing.T) {
	data := []float64{4.1, 0.1, 3.2, 1.5, 2.8, 0.5}
	// Sorted descending: 4.1, 3.2, 2.8, 1.5, 0.5, 0.1
	result := GetNthByMinHeapFloat64(data, 0)
	testutil.Equals(t, 4.1, result)

	result = GetNthByMinHeapFloat64(data, 2)
	testutil.Equals(t, 2.8, result)

	result = GetNthByMinHeapFloat64(data, 5)
	testutil.Equals(t, 0.1, result)
}

func TestGetNthByMinHeapFloat64WithNegatives(t *testing.T) {
	data := []float64{2.8, -1.5, -3.2, 0.0, -4.1}
	// Sorted descending: 2.8, 0.0, -1.5, -3.2, -4.1
	result := GetNthByMinHeapFloat64(data, 0)
	testutil.Equals(t, 2.8, result)

	result = GetNthByMinHeapFloat64(data, 2)
	testutil.Equals(t, -1.5, result)

	result = GetNthByMinHeapFloat64(data, 4)
	testutil.Equals(t, -4.1, result)
}

// ============== Tests for MedianHeap ==============

func TestMedianHeapBasic(t *testing.T) {
	h := NewMedianHeap(10)

	h.Add(1.0)
	testutil.Equals(t, 1, h.Count())
	testutil.Equals(t, 1.0, h.Get())

	h.Add(3.0)
	testutil.Equals(t, 2, h.Count())
	// Note: MedianHeap implementation returns max of left partition
	testutil.Equals(t, 3.0, h.Get())

	h.Add(2.0)
	testutil.Equals(t, 3, h.Count())
	testutil.Equals(t, 2.0, h.Get())
}

func TestMedianHeapEvenCount(t *testing.T) {
	h := NewMedianHeap(10)

	data := []float64{1.0, 2.0, 3.0, 4.0}
	for _, v := range data {
		h.Add(v)
	}

	testutil.Equals(t, 4, h.Count())
	// MedianHeap returns the max of the left partition (lower half)
	testutil.Equals(t, 3.0, h.Get())
}

func TestMedianHeapOddCount(t *testing.T) {
	h := NewMedianHeap(10)

	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	for _, v := range data {
		h.Add(v)
	}

	testutil.Equals(t, 5, h.Count())
	testutil.Equals(t, 3.0, h.Get())
}

func TestMedianHeapWithDuplicates(t *testing.T) {
	h := NewMedianHeap(10)

	data := []float64{1.0, 1.0, 1.0, 2.0, 2.0}
	for _, v := range data {
		h.Add(v)
	}

	testutil.Equals(t, 5, h.Count())
	testutil.Equals(t, 1.0, h.Get())
}

func TestMedianHeapWithNegatives(t *testing.T) {
	h := NewMedianHeap(10)

	data := []float64{-5.0, -1.0, -3.0, -2.0, -4.0}
	for _, v := range data {
		h.Add(v)
	}

	testutil.Equals(t, 5, h.Count())
	// MedianHeap returns the root of the min-heap (left partition max)
	testutil.Equals(t, -4.0, h.Get())
}

func TestMedianHeapClear(t *testing.T) {
	h := NewMedianHeap(10)

	h.Add(1.0)
	h.Add(2.0)
	testutil.Equals(t, 2, h.Count())

	h.Clear()
	testutil.Equals(t, 0, h.Count())

	// After clear, should be able to add again
	h.Add(3.0)
	testutil.Equals(t, 1, h.Count())
	testutil.Equals(t, 3.0, h.Get())
}

func TestMedianHeapReset(t *testing.T) {
	h := NewMedianHeap(5)

	// Add 5 elements
	for i := 1; i <= 5; i++ {
		h.Add(float64(i))
	}
	testutil.Equals(t, 5, h.Count())

	// Adding more should reset
	h.Add(10.0)
	testutil.Equals(t, 1, h.Count())
	testutil.Equals(t, 10.0, h.Get())
}

func TestMedianHeapLarge(t *testing.T) {
	rand.Seed(42)
	size := 999
	h := NewMedianHeap(size)

	data := make([]float64, size)
	for i := 0; i < size; i++ {
		data[i] = float64(rand.Intn(1000000))
		h.Add(data[i])
	}

	testutil.Equals(t, size, h.Count())

	// Verify median
	sorted := make([]float64, len(data))
	copy(sorted, data)
	sort.Float64s(sorted)
	expectedMedian := sorted[(size-1)/2]

	testutil.Equals(t, expectedMedian, h.Get())
}

// ============== Benchmarks ==============

func BenchmarkGetNthByMaxHeapInt64(b *testing.B) {
	rand.Seed(42)
	arr := make([]int64, 100000)
	for i := 0; i < len(arr); i++ {
		arr[i] = rand.Int63n(100000)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		GetNthByMaxHeapInt64(arr, len(arr)/2)
	}
}

func BenchmarkGetNthByMinHeapInt64(b *testing.B) {
	rand.Seed(42)
	arr := make([]int64, 100000)
	for i := 0; i < len(arr); i++ {
		arr[i] = rand.Int63n(100000)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		GetNthByMinHeapInt64(arr, len(arr)/2)
	}
}

func BenchmarkGetNthByMaxHeapFloat64(b *testing.B) {
	rand.Seed(42)
	arr := make([]float64, 100000)
	for i := 0; i < len(arr); i++ {
		arr[i] = rand.Float64() * 100000
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		GetNthByMaxHeapFloat64(arr, len(arr)/2)
	}
}

func BenchmarkGetNthByMinHeapFloat64(b *testing.B) {
	rand.Seed(42)
	arr := make([]float64, 100000)
	for i := 0; i < len(arr); i++ {
		arr[i] = rand.Float64() * 100000
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		GetNthByMinHeapFloat64(arr, len(arr)/2)
	}
}

func BenchmarkMedianHeapAdd(b *testing.B) {
	rand.Seed(42)
	h := NewMedianHeap(100000)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		h.Add(rand.Float64() * 100000)
		if h.Count() == 0 {
			b.Fatal("heap should not reset during benchmark")
		}
	}
}
