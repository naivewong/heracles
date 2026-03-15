package encoding

import (
	"fmt"
	"math/rand"
	// "sort"
	"testing"

	"github.com/naivewong/tsdb-group/testutil"
)

func BenchmarkMaxBits(bench *testing.B) {
	arr := make([]int64, 100000)
	for i := 0; i < len(arr); i++ {
		arr[i] = rand.Int63n(100000) - 50000
	}

	bench.ResetTimer()
	bench.ReportAllocs()
	for i := 0; i < bench.N; i++ {
		MaxBits(arr)
	}
}

func BenchmarkNth(bench *testing.B) {
	arr := make([]int64, 100000)
	for i := 0; i < len(arr); i++ {
		arr[i] = rand.Int63n(100000)
	}

	orders := make([]int, 10)
	for i := 0; i < len(orders); i++ {
		orders[i] = (i + 1) * len(arr) / (len(orders) + 1)
	}

	bench.Run("Heap", func(b *testing.B) {
		bench.ResetTimer()
		bench.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for j := 0; j < len(orders) / 2; j++ {
				GetNthByMaxHeapInt64(arr, orders[j])
				GetNthByMinHeapInt64(arr, orders[j])
			}
		}
	})
}

func TestHeap(test *testing.T) {
	l := []int64{10, 9, 8, 7, 6, 5, 4, 3, 2, 1}
	// GetNthByMaxHeapInt64 returns (k+1)th smallest element
	// Sorted: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
	testutil.Equals(test, int64(3), GetNthByMaxHeapInt64(l, 2)) // 3rd smallest
	testutil.Equals(test, int64(5), GetNthByMaxHeapInt64(l, 4)) // 5th smallest
	// GetNthByMinHeapInt64 returns (k+1)th largest element
	// Sorted descending: 10, 9, 8, 7, 6, 5, 4, 3, 2, 1
	testutil.Equals(test, int64(8), GetNthByMinHeapInt64(l, 2)) // 3rd largest
	testutil.Equals(test, int64(6), GetNthByMinHeapInt64(l, 4)) // 5th largest
}

func TestMedianHeap(test *testing.T) {
	l := []float64{1.2, 4.5, 2.3, 3.3, 5.6, 6.7, 7.6, 8.0, 9.0, 10.9}
	h := NewMedianHeap(10)
	h.Add(l[0])
	// testutil.Equals(test, l[0], h.Get())
	fmt.Println(h.Get())
	h.Add(l[1])
	// testutil.Equals(test, l[0], h.Get())
	fmt.Println(h.Get())
	h.Add(l[2])
	// testutil.Equals(test, l[1], h.Get())
	fmt.Println(h.Get())
	h.Add(l[3])
	// testutil.Equals(test, l[1], h.Get())
	fmt.Println(h.Get())
	h.Add(l[4])
	// testutil.Equals(test, l[1], h.Get())
	fmt.Println(h.Get())
	h.Add(l[5])
	// testutil.Equals(test, l[1], h.Get())
	fmt.Println(h.Get())
	h.Add(l[6])
	// testutil.Equals(test, l[1], h.Get())
	fmt.Println(h.Get())
	h.Add(l[7])
	// testutil.Equals(test, l[1], h.Get())
	fmt.Println(h.Get())
	h.Add(l[8])
	// testutil.Equals(test, l[1], h.Get())
	fmt.Println(h.Get())
	h.Add(l[9])
	// testutil.Equals(test, l[1], h.Get())
	fmt.Println(h.Get())
}

func TestMaxBits(test *testing.T) {
	// Test positive numbers
	testutil.Equals(test, 1, MaxBits([]int64{0}))
	testutil.Equals(test, 2, MaxBits([]int64{1}))
	testutil.Equals(test, 3, MaxBits([]int64{2}))
	testutil.Equals(test, 4, MaxBits([]int64{7}))

	// Test negative numbers
	testutil.Equals(test, 2, MaxBits([]int64{-1}))
	testutil.Equals(test, 3, MaxBits([]int64{-2}))

	// Test mixed numbers
	testutil.Equals(test, 3, MaxBits([]int64{-2, 1}))
	testutil.Equals(test, 4, MaxBits([]int64{-4, 3}))

	// Test larger numbers
	testutil.Equals(test, 32, MaxBits([]int64{1 << 30}))
	testutil.Equals(test, 64, MaxBits([]int64{1 << 62}))
}

func TestFromBits(test *testing.T) {
	// Test positive number decoding
	testutil.Equals(test, int64(5), FromBits(5, 4))

	// Test negative number decoding (sign bit is 1)
	testutil.Equals(test, int64(-3), FromBits(5, 3))  // 0b101 = 5, sign bit is 1
	testutil.Equals(test, int64(-1), FromBits(1, 1))  // 0b1 = 1, sign bit is 1
}

func TestMaxMinFromBitsLen(test *testing.T) {
	// Test MaxFromBitsLen
	testutil.Equals(test, int64(0), MaxFromBitsLen(1))
	testutil.Equals(test, int64(1), MaxFromBitsLen(2))
	testutil.Equals(test, int64(3), MaxFromBitsLen(3))
	testutil.Equals(test, int64(7), MaxFromBitsLen(4))

	// Test MinFromBitsLen
	testutil.Equals(test, int64(-1), MinFromBitsLen(1))
	testutil.Equals(test, int64(-2), MinFromBitsLen(2))
	testutil.Equals(test, int64(-4), MinFromBitsLen(3))
}
