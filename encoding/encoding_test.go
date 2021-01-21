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
	l := []int64{10,9,8,7,6,5,4,3,2,1}
	testutil.Equals(test, int64(2), GetNthByMaxHeapInt64(l, 2))
	testutil.Equals(test, int64(4), GetNthByMaxHeapInt64(l, 4))
	testutil.Equals(test, int64(9), GetNthByMinHeapInt64(l, 2))
	testutil.Equals(test, int64(7), GetNthByMinHeapInt64(l, 4))
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
