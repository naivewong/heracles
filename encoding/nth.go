package encoding

func heapParent(i int) int { return (i-1)>>1 }
func heapLeft(i int) int { return (i<<1)+1 }
func heapRight(i int) int { return (i<<1)+2 }

type maxHeapInt64 []int64
func (h maxHeapInt64) maxHeapify(i int) {
	l := heapLeft(i)
	r := heapRight(i)
	smallest := i
	if l < len(h) && h[l] > h[smallest] {
		smallest = l
	}
	if r < len(h) && h[r] > h[smallest] {
		smallest = r
	}
	if smallest != i {
		h[i], h[smallest] = h[smallest], h[i]
		h.maxHeapify(smallest)
	}
}

// It's user's responsibility to pass k < len(data).
func GetNthByMaxHeapInt64(data []int64, k int) int64 {
	h := make(maxHeapInt64, k + 1)
	for i := 0; i < k + 1; i++ {
		h[i] = data[i]
		j := i

		for j != 0 && h[heapParent(j)] < h[j] {
			h[heapParent(j)], h[j] = h[j], h[heapParent(j)]
			j = heapParent(j)
		}
	}
	for i := k + 1; i < len(data); i++ {
		if data[i] < h[0] {
			h[0] = data[i]
			h.maxHeapify(0)
		}
	}
	return h[0]
}

type minHeapInt64 []int64
func (h minHeapInt64) minHeapify(i int) {
	l := heapLeft(i)
	r := heapRight(i)
	smallest := i
	if l < len(h) && h[l] < h[smallest] {
		smallest = l
	}
	if r < len(h) && h[r] < h[smallest] {
		smallest = r
	}
	if smallest != i {
		h[i], h[smallest] = h[smallest], h[i]
		h.minHeapify(smallest)
	}
}

// It's user's responsibility to pass k < len(data).
func GetNthByMinHeapInt64(data []int64, k int) int64 {
	h := make(minHeapInt64, k + 1)
	for i := 0; i < k + 1; i++ {
		h[i] = data[i]
		j := i

		for j != 0 && h[heapParent(j)] > h[j] {
			h[heapParent(j)], h[j] = h[j], h[heapParent(j)]
			j = heapParent(j)
		}
	}
	for i := k + 1; i < len(data); i++ {
		if data[i] > h[0] {
			h[0] = data[i]
			h.minHeapify(0)
		}
	}
	return h[0]
}

type maxHeapFloat64 []float64
func (h maxHeapFloat64) maxHeapify(i int) {
	l := heapLeft(i)
	r := heapRight(i)
	smallest := i
	if l < len(h) && h[l] > h[smallest] {
		smallest = l
	}
	if r < len(h) && h[r] > h[smallest] {
		smallest = r
	}
	if smallest != i {
		h[i], h[smallest] = h[smallest], h[i]
		h.maxHeapify(smallest)
	}
}

// It's user's responsibility to pass k < len(data).
func GetNthByMaxHeapFloat64(data []float64, k int) float64 {
	h := make(maxHeapFloat64, k + 1)
	for i := 0; i < k + 1; i++ {
		h[i] = data[i]
		j := i

		for j != 0 && h[heapParent(j)] < h[j] {
			h[heapParent(j)], h[j] = h[j], h[heapParent(j)]
			j = heapParent(j)
		}
	}
	for i := k + 1; i < len(data); i++ {
		if data[i] < h[0] {
			h[0] = data[i]
			h.maxHeapify(0)
		}
	}
	return h[0]
}

type minHeapFloat64 []float64
func (h minHeapFloat64) minHeapify(i int) {
	l := heapLeft(i)
	r := heapRight(i)
	smallest := i
	if l < len(h) && h[l] < h[smallest] {
		smallest = l
	}
	if r < len(h) && h[r] < h[smallest] {
		smallest = r
	}
	if smallest != i {
		h[i], h[smallest] = h[smallest], h[i]
		h.minHeapify(smallest)
	}
}

// It's user's responsibility to pass k < len(data).
func GetNthByMinHeapFloat64(data []float64, k int) float64 {
	h := make(minHeapFloat64, k + 1)
	for i := 0; i < k + 1; i++ {
		h[i] = data[i]
		j := i

		for j != 0 && h[heapParent(j)] > h[j] {
			h[heapParent(j)], h[j] = h[j], h[heapParent(j)]
			j = heapParent(j)
		}
	}
	for i := k + 1; i < len(data); i++ {
		if data[i] > h[0] {
			h[0] = data[i]
			h.minHeapify(0)
		}
	}
	return h[0]
}

type MedianHeap struct {
	arr   []float64
	size  int
	count int
	k     int
}

func NewMedianHeap(size int) *MedianHeap { return &MedianHeap{arr: make([]float64, 0, size), size: size} }

func (h *MedianHeap) minHeapify(i int) {
	l := heapLeft(i)
	r := heapRight(i)
	smallest := i
	if l < h.k && h.arr[l] < h.arr[smallest] {
		smallest = l
	}
	if r < h.k && h.arr[r] < h.arr[smallest] {
		smallest = r
	}
	if smallest != i {
		h.arr[i], h.arr[smallest] = h.arr[smallest], h.arr[i]
		h.minHeapify(smallest)
	}
}

func (h *MedianHeap) Add(v float64) {
	if h.count == h.size {
		h.arr = h.arr[:0]
		h.count = 0
		h.k = 0
	}
	h.arr = append(h.arr, v)
	h.count++
	h.k = (h.count+1)>>1
	j := h.k - 1
	for j != 0 && h.arr[heapParent(j)] > h.arr[j] {
		h.arr[heapParent(j)], h.arr[j] = h.arr[j], h.arr[heapParent(j)]
		j = heapParent(j)
	}
	if h.arr[len(h.arr) - 1] > h.arr[0] {
		h.arr[0], h.arr[len(h.arr) - 1] = h.arr[len(h.arr) - 1], h.arr[0]
		h.minHeapify(0)
	}
}

// It's user's responsibility to call Get under a not empty heap.
func (h *MedianHeap) Get() float64 { return h.arr[0] }

func (h *MedianHeap) Count() int { return h.count }

func (h *MedianHeap) Clear() {
	h.arr = h.arr[:0]
	h.count = 0
	h.k = 0
}
