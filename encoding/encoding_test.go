package encoding

import (
	"fmt"
	"hash/crc32"
	"math/rand"
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

func TestEncbuf(t *testing.T) {
	var buf Encbuf

	// Test Reset, Get, Len
	testutil.Equals(t, 0, buf.Len())

	// Test PutByte
	buf.PutByte(0x42)
	testutil.Equals(t, 1, buf.Len())
	testutil.Equals(t, []byte{0x42}, buf.Get())

	// Test Reset
	buf.Reset()
	testutil.Equals(t, 0, buf.Len())

	// Test PutString
	buf.PutString("hello")
	testutil.Equals(t, 5, buf.Len())
	testutil.Equals(t, "hello", string(buf.Get()))

	// Test PutBE32
	buf.Reset()
	buf.PutBE32(0x12345678)
	testutil.Equals(t, 4, buf.Len())

	// Test PutBE64
	buf.Reset()
	buf.PutBE64(0x123456789ABCDEF0)
	testutil.Equals(t, 8, buf.Len())

	// Test PutUvarint64
	buf.Reset()
	buf.PutUvarint64(300)
	testutil.Assert(t, buf.Len() > 0, "should have written bytes")

	// Test PutVarint64
	buf.Reset()
	buf.PutVarint64(-100)
	testutil.Assert(t, buf.Len() > 0, "should have written bytes")

	// Test PutBE32int
	buf.Reset()
	buf.PutBE32int(42)
	testutil.Equals(t, 4, buf.Len())

	// Test PutUvarint32
	buf.Reset()
	buf.PutUvarint32(100)
	testutil.Assert(t, buf.Len() > 0, "should have written bytes")

	// Test PutBE64int64
	buf.Reset()
	buf.PutBE64int64(-12345)
	testutil.Equals(t, 8, buf.Len())

	// Test PutUvarint
	buf.Reset()
	buf.PutUvarint(42)
	testutil.Assert(t, buf.Len() > 0, "should have written bytes")

	// Test PutUvarintStr
	buf.Reset()
	buf.PutUvarintStr("test")
	testutil.Assert(t, buf.Len() > 0, "should have written bytes")

	// Test PutHash
	buf.Reset()
	buf.PutString("test")
	buf.PutHash(crc32.New(crc32.MakeTable(crc32.Castagnoli)))
	testutil.Equals(t, 8, buf.Len()) // 4 bytes string + 4 bytes CRC
}

func TestDecbuf(t *testing.T) {
	var buf Encbuf

	// Write some test data
	buf.PutBE32(0x12345678)
	buf.PutBE64(0x123456789ABCDEF0)
	buf.PutByte(0x42)
	buf.PutUvarint64(300)
	buf.PutVarint64(-100)

	// Create Decbuf
	db := Decbuf{B: buf.Get()}

	// Test Be32
	testutil.Equals(t, uint32(0x12345678), db.Be32())

	// Test Be64
	testutil.Equals(t, uint64(0x123456789ABCDEF0), db.Be64())

	// Test Byte
	testutil.Equals(t, byte(0x42), db.Byte())

	// Test Uvarint64
	testutil.Equals(t, uint64(300), db.Uvarint64())

	// Test Varint64
	testutil.Equals(t, int64(-100), db.Varint64())

	// Test Err
	testutil.Ok(t, db.Err())

	// Test Len
	remaining := db.Len()
	testutil.Assert(t, remaining >= 0, "length should be non-negative")
}

func TestDecbufUvarint(t *testing.T) {
	var buf Encbuf
	buf.PutUvarint64(12345)

	db := Decbuf{B: buf.Get()}
	testutil.Equals(t, int(12345), db.Uvarint())
	testutil.Ok(t, db.Err())
}

func TestDecbufBe32int(t *testing.T) {
	var buf Encbuf
	buf.PutBE32int(42)

	db := Decbuf{B: buf.Get()}
	testutil.Equals(t, int(42), db.Be32int())
	testutil.Ok(t, db.Err())
}

func TestDecbufBe64int64(t *testing.T) {
	var buf Encbuf
	buf.PutBE64int64(-12345)

	db := Decbuf{B: buf.Get()}
	testutil.Equals(t, int64(-12345), db.Be64int64())
	testutil.Ok(t, db.Err())
}

func TestDecbufUvarintStr(t *testing.T) {
	var buf Encbuf
	buf.PutUvarintStr("hello world")

	db := Decbuf{B: buf.Get()}
	testutil.Equals(t, "hello world", db.UvarintStr())
	testutil.Ok(t, db.Err())
}

func TestDecbufCrc32(t *testing.T) {
	var buf Encbuf
	buf.PutString("test")
	table := crc32.MakeTable(crc32.Castagnoli)
	hash := crc32.New(table)
	hash.Write([]byte("test"))
	expectedCrc := hash.Sum32()

	db := Decbuf{B: buf.Get()}
	testutil.Equals(t, expectedCrc, db.Crc32(table))
}

func TestDecbufGet(t *testing.T) {
	var buf Encbuf
	buf.PutString("test")

	db := Decbuf{B: buf.Get()}
	data := db.Get()
	testutil.Equals(t, "test", string(data))
}
