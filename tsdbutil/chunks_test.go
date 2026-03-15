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

package tsdbutil

import (
	"testing"

	"github.com/naivewong/tsdb-group/testutil"
)

// NOTE: sample type is already defined in buffer.go
// type sample struct { t int64; v float64 }

// ============== Tests for ChunkFromSamples ==============

func TestChunkFromSamples(t *testing.T) {
	t.Run("empty samples", func(t *testing.T) {
		samples := []Sample{}
		chunk := ChunkFromSamples(samples)

		testutil.Equals(t, int64(0), chunk.MinTime)
		testutil.Equals(t, int64(0), chunk.MaxTime)
		testutil.Equals(t, 0, chunk.Chunk.NumSamples())
	})

	t.Run("single sample", func(t *testing.T) {
		samples := []Sample{sample{t: 100, v: 1.5}}
		chunk := ChunkFromSamples(samples)

		testutil.Equals(t, int64(100), chunk.MinTime)
		testutil.Equals(t, int64(100), chunk.MaxTime)
		testutil.Equals(t, 1, chunk.Chunk.NumSamples())

		// Verify the sample can be read back
		it := chunk.Chunk.Iterator()
		testutil.Equals(t, true, it.Next())
		ts, v := it.At()
		testutil.Equals(t, int64(100), ts)
		testutil.Equals(t, 1.5, v)
		testutil.Equals(t, false, it.Next())
	})

	t.Run("multiple samples", func(t *testing.T) {
		samples := []Sample{
			sample{t: 100, v: 1.0},
			sample{t: 200, v: 2.0},
			sample{t: 300, v: 3.0},
		}
		chunk := ChunkFromSamples(samples)

		testutil.Equals(t, int64(100), chunk.MinTime)
		testutil.Equals(t, int64(300), chunk.MaxTime)
		testutil.Equals(t, 3, chunk.Chunk.NumSamples())

		// Verify samples can be read back
		it := chunk.Chunk.Iterator()
		for _, expected := range samples {
			testutil.Equals(t, true, it.Next())
			ts, v := it.At()
			testutil.Equals(t, expected.T(), ts)
			testutil.Equals(t, expected.V(), v)
		}
		testutil.Equals(t, false, it.Next())
	})

	t.Run("negative timestamps", func(t *testing.T) {
		samples := []Sample{
			sample{t: -100, v: 1.0},
			sample{t: -50, v: 2.0},
			sample{t: 0, v: 3.0},
		}
		chunk := ChunkFromSamples(samples)

		testutil.Equals(t, int64(-100), chunk.MinTime)
		testutil.Equals(t, int64(0), chunk.MaxTime)
		testutil.Equals(t, 3, chunk.Chunk.NumSamples())
	})

	t.Run("negative values", func(t *testing.T) {
		samples := []Sample{
			sample{t: 100, v: -1.0},
			sample{t: 200, v: -2.5},
			sample{t: 300, v: -3.5},
		}
		chunk := ChunkFromSamples(samples)

		testutil.Equals(t, int64(100), chunk.MinTime)
		testutil.Equals(t, int64(300), chunk.MaxTime)

		it := chunk.Chunk.Iterator()
		for _, expected := range samples {
			testutil.Equals(t, true, it.Next())
			ts, v := it.At()
			testutil.Equals(t, expected.T(), ts)
			testutil.Equals(t, expected.V(), v)
		}
	})

	t.Run("zero values", func(t *testing.T) {
		samples := []Sample{
			sample{t: 100, v: 0.0},
			sample{t: 200, v: 0.0},
			sample{t: 300, v: 0.0},
		}
		chunk := ChunkFromSamples(samples)

		testutil.Equals(t, int64(100), chunk.MinTime)
		testutil.Equals(t, int64(300), chunk.MaxTime)
		testutil.Equals(t, 3, chunk.Chunk.NumSamples())
	})

	t.Run("large timestamps", func(t *testing.T) {
		samples := []Sample{
			sample{t: 1000000, v: 1.0},
			sample{t: 2000000, v: 2.0},
		}
		chunk := ChunkFromSamples(samples)

		testutil.Equals(t, int64(1000000), chunk.MinTime)
		testutil.Equals(t, int64(2000000), chunk.MaxTime)
	})

	t.Run("unordered timestamps", func(t *testing.T) {
		// Note: samples should typically be in order, but we test the behavior
		samples := []Sample{
			sample{t: 300, v: 3.0},
			sample{t: 100, v: 1.0},
			sample{t: 200, v: 2.0},
		}
		chunk := ChunkFromSamples(samples)

		// MinTime and MaxTime are based on first and last sample
		testutil.Equals(t, int64(300), chunk.MinTime)
		testutil.Equals(t, int64(200), chunk.MaxTime)
	})
}

// ============== Tests for PopulatedChunk ==============

func TestPopulatedChunk(t *testing.T) {
	t.Run("zero samples", func(t *testing.T) {
		chunk := PopulatedChunk(0, 100)

		testutil.Equals(t, int64(0), chunk.MinTime)
		testutil.Equals(t, int64(0), chunk.MaxTime)
		testutil.Equals(t, 0, chunk.Chunk.NumSamples())
	})

	t.Run("single sample", func(t *testing.T) {
		chunk := PopulatedChunk(1, 100)

		testutil.Equals(t, int64(100), chunk.MinTime)
		testutil.Equals(t, int64(100), chunk.MaxTime)
		testutil.Equals(t, 1, chunk.Chunk.NumSamples())

		it := chunk.Chunk.Iterator()
		testutil.Equals(t, true, it.Next())
		ts, v := it.At()
		testutil.Equals(t, int64(100), ts)
		testutil.Equals(t, 1.0, v)
		testutil.Equals(t, false, it.Next())
	})

	t.Run("multiple samples", func(t *testing.T) {
		numSamples := 5
		minTime := int64(1000)
		chunk := PopulatedChunk(numSamples, minTime)

		testutil.Equals(t, minTime, chunk.MinTime)
		testutil.Equals(t, minTime+int64((numSamples-1)*1000), chunk.MaxTime)
		testutil.Equals(t, numSamples, chunk.Chunk.NumSamples())

		it := chunk.Chunk.Iterator()
		for i := 0; i < numSamples; i++ {
			testutil.Equals(t, true, it.Next())
			ts, v := it.At()
			testutil.Equals(t, minTime+int64(i*1000), ts)
			testutil.Equals(t, 1.0, v)
		}
		testutil.Equals(t, false, it.Next())
	})

	t.Run("large number of samples", func(t *testing.T) {
		numSamples := 1000
		minTime := int64(0)
		chunk := PopulatedChunk(numSamples, minTime)

		testutil.Equals(t, numSamples, chunk.Chunk.NumSamples())
		testutil.Equals(t, minTime, chunk.MinTime)
		testutil.Equals(t, minTime+int64((numSamples-1)*1000), chunk.MaxTime)

		// Count samples via iterator
		count := 0
		it := chunk.Chunk.Iterator()
		for it.Next() {
			count++
		}
		testutil.Equals(t, numSamples, count)
	})

	t.Run("negative minTime", func(t *testing.T) {
		numSamples := 5
		minTime := int64(-1000)
		chunk := PopulatedChunk(numSamples, minTime)

		testutil.Equals(t, minTime, chunk.MinTime)
		testutil.Equals(t, minTime+int64((numSamples-1)*1000), chunk.MaxTime)

		it := chunk.Chunk.Iterator()
		for i := 0; i < numSamples; i++ {
			testutil.Equals(t, true, it.Next())
			ts, v := it.At()
			testutil.Equals(t, minTime+int64(i*1000), ts)
			testutil.Equals(t, 1.0, v)
		}
	})

	t.Run("all values are 1.0", func(t *testing.T) {
		chunk := PopulatedChunk(10, 0)

		it := chunk.Chunk.Iterator()
		for it.Next() {
			_, v := it.At()
			testutil.Equals(t, 1.0, v)
		}
	})

	t.Run("samples are 1 second apart", func(t *testing.T) {
		numSamples := 10
		minTime := int64(5000)
		chunk := PopulatedChunk(numSamples, minTime)

		it := chunk.Chunk.Iterator()
		var prevTs int64 = -1
		for it.Next() {
			ts, _ := it.At()
			if prevTs >= 0 {
				testutil.Equals(t, int64(1000), ts-prevTs)
			}
			prevTs = ts
		}
	})
}

// ============== Benchmarks ==============

func BenchmarkChunkFromSamples(b *testing.B) {
	samples := make([]Sample, 100)
	for i := 0; i < 100; i++ {
		samples[i] = sample{t: int64(i * 1000), v: float64(i)}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ChunkFromSamples(samples)
	}
}

func BenchmarkChunkFromSamplesLarge(b *testing.B) {
	samples := make([]Sample, 10000)
	for i := 0; i < 10000; i++ {
		samples[i] = sample{t: int64(i * 1000), v: float64(i)}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ChunkFromSamples(samples)
	}
}

func BenchmarkPopulatedChunk(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		PopulatedChunk(1000, 0)
	}
}
