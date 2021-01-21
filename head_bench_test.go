// Copyright 2018 The Prometheus Authors
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
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
// 	"sync/atomic"
	"testing"
	"time"

	"github.com/naivewong/tsdb-group/labels"
	"github.com/naivewong/tsdb-group/testutil"
	tsdb_origin "github.com/prometheus/tsdb"
	origin_labels "github.com/prometheus/tsdb/labels"
)

func TestHeadComparison(t *testing.T) {
	devMag := 10
	{
		lsets := []origin_labels.Labels{}
		queryLabels := []origin_labels.Label{}
		LoadDevOpsSeries(t, &lsets, &queryLabels, 5000, 100, false)
		h, err := tsdb_origin.NewHead(nil, nil, nil, 2*3600*1000)
		testutil.Ok(t, err)
		defer h.Close()

		refs := make([]uint64, len(lsets))
		for i := 0; i < len(refs); i++ {
			refs[i] = 0
		}

		totalSamples := 0
		for j := 0; j < 3600*2-1; j++ {
			t_ := int64(1000) * int64(j) + int64(rand.Float64()*float64(devMag))
			app := h.Appender()
			for i := 0; i < len(lsets); i++ {
				if refs[i] == 0 {
					ref, err := app.Add(lsets[i], t_, rand.Float64())
					testutil.Ok(t, err)
					refs[i] = ref
				} else {
					err := app.AddFast(refs[i], t_, rand.Float64())
					testutil.Ok(t, err)
				}
			}
			totalSamples += len(lsets)
			err := app.Commit()
			testutil.Ok(t, err)
		}
		fmt.Println("total appended samples", totalSamples)

		t.Run("Head Random 50s interval", func(t *testing.T) {
			starts := []int64{}
			ends := []int64{}
			for i := 0; i < 3600*2; i += 50 {
				starts = append(starts, int64(i * 1000))
				ends = append(ends, int64((i + 50) * 1000))
			}

			// Prepare matchers.
			matchers := []origin_labels.Matcher{}
			for _, l := range queryLabels {
				matchers = append(matchers, origin_labels.NewEqualMatcher(l.Name, l.Value))
			}

			totalSamples = 0
			start := time.Now()
			for i := 0; i < len(starts); i++ {
				q, err := tsdb_origin.NewBlockQuerier(h, starts[i], ends[i])
				testutil.Ok(t, err)

				for _, matcher := range matchers {
					ss, err := q.Select(matcher)
					testutil.Ok(t, err)

					for ss.Next() {
						it := ss.At().Iterator()
						for it.Next() {
							totalSamples += 1
						}
					}
				}
				q.Close()
			}
			fmt.Println("total query samples", totalSamples)
			fmt.Printf("Head random 100s interval duration=%f\n", time.Since(start).Seconds())
		})
		t.Run("Head Random 100s interval", func(t *testing.T) {
			starts := []int64{}
			ends := []int64{}
			for i := 0; i < 3600*2; i += 100 {
				starts = append(starts, int64(i * 1000))
				ends = append(ends, int64((i + 100) * 1000))
			}

			// Prepare matchers.
			matchers := []origin_labels.Matcher{}
			for _, l := range queryLabels {
				matchers = append(matchers, origin_labels.NewEqualMatcher(l.Name, l.Value))
			}

			totalSamples = 0
			start := time.Now()
			for i := 0; i < len(starts); i++ {
				q, err := tsdb_origin.NewBlockQuerier(h, starts[i], ends[i])
				testutil.Ok(t, err)

				for _, matcher := range matchers {
					ss, err := q.Select(matcher)
					testutil.Ok(t, err)

					for ss.Next() {
						it := ss.At().Iterator()
						for it.Next() {
							totalSamples += 1
						}
					}
				}
				q.Close()
			}
			fmt.Println("total query samples", totalSamples)
			fmt.Printf("Head random 100s interval duration=%f\n", time.Since(start).Seconds())
		})
		t.Run("Head Random 300s interval", func(t *testing.T) {
			starts := []int64{}
			ends := []int64{}
			for i := 0; i < 3600*2; i += 300 {
				starts = append(starts, int64(i * 1000))
				ends = append(ends, int64((i + 300) * 1000))
			}

			// Prepare matchers.
			matchers := []origin_labels.Matcher{}
			for _, l := range queryLabels {
				matchers = append(matchers, origin_labels.NewEqualMatcher(l.Name, l.Value))
			}

			totalSamples = 0
			start := time.Now()
			for i := 0; i < len(starts); i++ {
				q, err := tsdb_origin.NewBlockQuerier(h, starts[i], ends[i])
				testutil.Ok(t, err)

				for _, matcher := range matchers {
					ss, err := q.Select(matcher)
					testutil.Ok(t, err)

					for ss.Next() {
						it := ss.At().Iterator()
						for it.Next() {
							totalSamples += 1
						}
					}
				}
				q.Close()
			}
			fmt.Println("total query samples", totalSamples)
			fmt.Printf("Head random 300s interval duration=%f\n", time.Since(start).Seconds())
		})
		t.Run("Head Random 500s interval", func(t *testing.T) {
			starts := []int64{}
			ends := []int64{}
			for i := 0; i < 3600*2; i += 500 {
				starts = append(starts, int64(i * 1000))
				ends = append(ends, int64((i + 500) * 1000))
			}

			// Prepare matchers.
			matchers := []origin_labels.Matcher{}
			for _, l := range queryLabels {
				matchers = append(matchers, origin_labels.NewEqualMatcher(l.Name, l.Value))
			}

			totalSamples = 0
			start := time.Now()
			for i := 0; i < len(starts); i++ {
				q, err := tsdb_origin.NewBlockQuerier(h, starts[i], ends[i])
				testutil.Ok(t, err)

				for _, matcher := range matchers {
					ss, err := q.Select(matcher)
					testutil.Ok(t, err)

					for ss.Next() {
						it := ss.At().Iterator()
						for it.Next() {
							totalSamples += 1
						}
					}
				}
				q.Close()
			}
			fmt.Println("total query samples", totalSamples)
			fmt.Printf("Head random 500s interval duration=%f\n", time.Since(start).Seconds())
		})
	}
		
	{
		lsets := []labels.Labels{}
		queryLabels := []labels.Label{}
		GroupLoadDevOpsSeries(t, &lsets, &queryLabels, 101, 5000, 100, false)
		h, err := NewHead(nil, nil, nil, 2*3600*1000)
		testutil.Ok(t, err)
		defer h.Close()

		refs := make([]uint64, (len(lsets)+100)/101)
		for i := 0; i < len(refs); i++ {
			refs[i] = 0
		}

		totalSamples := 0
		for j := 0; j < 3600*2-1; j++ {
			t_ := int64(1000) * int64(j) + int64(rand.Float64()*float64(devMag))
			app := h.Appender()
			for i := 0; i < len(lsets); i += 101 {
				tempVals := make([]float64, 101)
				for k := 0; k < len(tempVals); k++ {
					tempVals[k] = rand.Float64()
				}
				if refs[i/101] == 0 {
					ref, err := app.AddGroup(lsets[i:i+101], t_, tempVals)
					testutil.Ok(t, err)
					refs[i/101] = ref
				} else {
					err := app.AddGroupFast(refs[i/101], t_, tempVals)
					testutil.Ok(t, err)
				}
			}
			totalSamples += len(lsets)
			err := app.Commit()
			testutil.Ok(t, err)
		}
		fmt.Println("total appended samples", totalSamples)

		t.Run("Group Head Random 50s interval", func(t *testing.T) {
			starts := []int64{}
			ends := []int64{}
			for i := 0; i < 3600*2; i += 50 {
				starts = append(starts, int64(i * 1000))
				ends = append(ends, int64((i + 50) * 1000))
			}

			// Prepare matchers.
			matchers := []labels.Matcher{}
			for _, l := range queryLabels {
				matchers = append(matchers, labels.NewEqualMatcher(l.Name, l.Value))
			}

			totalSamples = 0
			start := time.Now()
			for i := 0; i < len(starts); i++ {
				q, err := NewBlockQuerier(h, starts[i], ends[i])
				testutil.Ok(t, err)

				for _, matcher := range matchers {
					ss, err := q.Select(matcher)
					testutil.Ok(t, err)

					for ss.Next() {
						it := ss.At().Iterator()
						for it.Next() {
							totalSamples += 1
						}
					}
				}
				q.Close()
			}
			fmt.Println("total query samples", totalSamples)
			fmt.Printf("Group head random 100s interval duration=%f\n", time.Since(start).Seconds())
		})
		t.Run("Group Head Random 100s interval", func(t *testing.T) {
			starts := []int64{}
			ends := []int64{}
			for i := 0; i < 3600*2; i += 100 {
				starts = append(starts, int64(i * 1000))
				ends = append(ends, int64((i + 100) * 1000))
			}

			// Prepare matchers.
			matchers := []labels.Matcher{}
			for _, l := range queryLabels {
				matchers = append(matchers, labels.NewEqualMatcher(l.Name, l.Value))
			}

			totalSamples = 0
			start := time.Now()
			for i := 0; i < len(starts); i++ {
				q, err := NewBlockQuerier(h, starts[i], ends[i])
				testutil.Ok(t, err)

				for _, matcher := range matchers {
					ss, err := q.Select(matcher)
					testutil.Ok(t, err)

					for ss.Next() {
						it := ss.At().Iterator()
						for it.Next() {
							totalSamples += 1
						}
					}
				}
				q.Close()
			}
			fmt.Println("total query samples", totalSamples)
			fmt.Printf("Group head random 100s interval duration=%f\n", time.Since(start).Seconds())
		})
		t.Run("Group Head Random 300s interval", func(t *testing.T) {
			starts := []int64{}
			ends := []int64{}
			for i := 0; i < 3600*2; i += 300 {
				starts = append(starts, int64(i * 1000))
				ends = append(ends, int64((i + 300) * 1000))
			}

			// Prepare matchers.
			matchers := []labels.Matcher{}
			for _, l := range queryLabels {
				matchers = append(matchers, labels.NewEqualMatcher(l.Name, l.Value))
			}

			totalSamples = 0
			start := time.Now()
			for i := 0; i < len(starts); i++ {
				q, err := NewBlockQuerier(h, starts[i], ends[i])
				testutil.Ok(t, err)

				for _, matcher := range matchers {
					ss, err := q.Select(matcher)
					testutil.Ok(t, err)

					for ss.Next() {
						it := ss.At().Iterator()
						for it.Next() {
							totalSamples += 1
						}
					}
				}
				q.Close()
			}
			fmt.Println("total query samples", totalSamples)
			fmt.Printf("Group head random 300s interval duration=%f\n", time.Since(start).Seconds())
		})
		t.Run("Group Head Random 500s interval", func(t *testing.T) {
			starts := []int64{}
			ends := []int64{}
			for i := 0; i < 3600*2; i += 500 {
				starts = append(starts, int64(i * 1000))
				ends = append(ends, int64((i + 500) * 1000))
			}

			// Prepare matchers.
			matchers := []labels.Matcher{}
			for _, l := range queryLabels {
				matchers = append(matchers, labels.NewEqualMatcher(l.Name, l.Value))
			}

			totalSamples = 0
			start := time.Now()
			for i := 0; i < len(starts); i++ {
				q, err := NewBlockQuerier(h, starts[i], ends[i])
				testutil.Ok(t, err)

				for _, matcher := range matchers {
					ss, err := q.Select(matcher)
					testutil.Ok(t, err)

					for ss.Next() {
						it := ss.At().Iterator()
						for it.Next() {
							totalSamples += 1
						}
					}
				}
				q.Close()
			}
			fmt.Println("total query samples", totalSamples)
			fmt.Printf("Group head random 500s interval duration=%f\n", time.Since(start).Seconds())
		})
	}

	{
		lsets := []origin_labels.Labels{}
		queryLabels := []origin_labels.Label{}
		LoadDevOpsSeries(t, &lsets, &queryLabels, 5000, 100, false)
		h, err := tsdb_origin.NewHead(nil, nil, nil, 2*3600*1000)
		testutil.Ok(t, err)
		defer h.Close()

		refs := make([]uint64, len(lsets))
		for i := 0; i < len(refs); i++ {
			refs[i] = 0
		}

		file, err := os.Open("./testdata/bigdata/data50_12.txt")
		testutil.Ok(t, err)
		scanner := bufio.NewScanner(file)
		totalSamples := 0
		k := 0
		for scanner.Scan() && k < 3600*2-1 {
			t_ := int64(1000) * int64(k) + int64(rand.Float64()*float64(devMag))
			app := h.Appender()
			s := scanner.Text()
			data := strings.Split(s, ",")
			for i := 0; i < len(lsets); i++ {
				v, _ := strconv.ParseFloat(data[i], 64)
				if refs[i] == 0 {
					ref, err := app.Add(lsets[i], t_, v)
					testutil.Ok(t, err)
					refs[i] = ref
				} else {
					err := app.AddFast(refs[i], t_, v)
					testutil.Ok(t, err)
				}
			}
			totalSamples += len(lsets)
			err := app.Commit()
			testutil.Ok(t, err)

			k += 1
		}
		fmt.Println("total appended samples", totalSamples)

		t.Run("Head Timeseries 50s interval", func(t *testing.T) {
			starts := []int64{}
			ends := []int64{}
			for i := 0; i < 3600*2; i += 50 {
				starts = append(starts, int64(i * 1000))
				ends = append(ends, int64((i + 50) * 1000))
			}

			// Prepare matchers.
			matchers := []origin_labels.Matcher{}
			for _, l := range queryLabels {
				matchers = append(matchers, origin_labels.NewEqualMatcher(l.Name, l.Value))
			}

			totalSamples = 0
			start := time.Now()
			for i := 0; i < len(starts); i++ {
				q, err := tsdb_origin.NewBlockQuerier(h, starts[i], ends[i])
				testutil.Ok(t, err)

				for _, matcher := range matchers {
					ss, err := q.Select(matcher)
					testutil.Ok(t, err)

					for ss.Next() {
						it := ss.At().Iterator()
						for it.Next() {
							totalSamples += 1
						}
					}
				}
				q.Close()
			}
			fmt.Println("total query samples", totalSamples)
			fmt.Printf("Head timeseries 100s interval duration=%f\n", time.Since(start).Seconds())
		})
		t.Run("Head Timeseries 100s interval", func(t *testing.T) {
			starts := []int64{}
			ends := []int64{}
			for i := 0; i < 3600*2; i += 100 {
				starts = append(starts, int64(i * 1000))
				ends = append(ends, int64((i + 100) * 1000))
			}

			// Prepare matchers.
			matchers := []origin_labels.Matcher{}
			for _, l := range queryLabels {
				matchers = append(matchers, origin_labels.NewEqualMatcher(l.Name, l.Value))
			}

			totalSamples = 0
			start := time.Now()
			for i := 0; i < len(starts); i++ {
				q, err := tsdb_origin.NewBlockQuerier(h, starts[i], ends[i])
				testutil.Ok(t, err)

				for _, matcher := range matchers {
					ss, err := q.Select(matcher)
					testutil.Ok(t, err)

					for ss.Next() {
						it := ss.At().Iterator()
						for it.Next() {
							totalSamples += 1
						}
					}
				}
				q.Close()
			}
			fmt.Println("total query samples", totalSamples)
			fmt.Printf("Head timeseries 100s interval duration=%f\n", time.Since(start).Seconds())
		})
		t.Run("Head Timeseries 300s interval", func(t *testing.T) {
			starts := []int64{}
			ends := []int64{}
			for i := 0; i < 3600*2; i += 300 {
				starts = append(starts, int64(i * 1000))
				ends = append(ends, int64((i + 300) * 1000))
			}

			// Prepare matchers.
			matchers := []origin_labels.Matcher{}
			for _, l := range queryLabels {
				matchers = append(matchers, origin_labels.NewEqualMatcher(l.Name, l.Value))
			}

			totalSamples = 0
			start := time.Now()
			for i := 0; i < len(starts); i++ {
				q, err := tsdb_origin.NewBlockQuerier(h, starts[i], ends[i])
				testutil.Ok(t, err)

				for _, matcher := range matchers {
					ss, err := q.Select(matcher)
					testutil.Ok(t, err)

					for ss.Next() {
						it := ss.At().Iterator()
						for it.Next() {
							totalSamples += 1
						}
					}
				}
				q.Close()
			}
			fmt.Println("total query samples", totalSamples)
			fmt.Printf("Head timeseries 300s interval duration=%f\n", time.Since(start).Seconds())
		})
		t.Run("Head Timeseries 500s interval", func(t *testing.T) {
			starts := []int64{}
			ends := []int64{}
			for i := 0; i < 3600*2; i += 500 {
				starts = append(starts, int64(i * 1000))
				ends = append(ends, int64((i + 500) * 1000))
			}

			// Prepare matchers.
			matchers := []origin_labels.Matcher{}
			for _, l := range queryLabels {
				matchers = append(matchers, origin_labels.NewEqualMatcher(l.Name, l.Value))
			}

			totalSamples = 0
			start := time.Now()
			for i := 0; i < len(starts); i++ {
				q, err := tsdb_origin.NewBlockQuerier(h, starts[i], ends[i])
				testutil.Ok(t, err)

				for _, matcher := range matchers {
					ss, err := q.Select(matcher)
					testutil.Ok(t, err)

					for ss.Next() {
						it := ss.At().Iterator()
						for it.Next() {
							totalSamples += 1
						}
					}
				}
				q.Close()
			}
			fmt.Println("total query samples", totalSamples)
			fmt.Printf("Head timeseries 500s interval duration=%f\n", time.Since(start).Seconds())
		})
	}
		
	{
		lsets := []labels.Labels{}
		queryLabels := []labels.Label{}
		GroupLoadDevOpsSeries(t, &lsets, &queryLabels, 101, 5000, 100, false)
		h, err := NewHead(nil, nil, nil, 2*3600*1000)
		testutil.Ok(t, err)
		defer h.Close()

		refs := make([]uint64, (len(lsets)+100)/101)
		for i := 0; i < len(refs); i++ {
			refs[i] = 0
		}

		file, err := os.Open("./testdata/bigdata/data50_12.txt")
		testutil.Ok(t, err)
		scanner := bufio.NewScanner(file)
		totalSamples := 0
		k := 0
		for scanner.Scan() && k < 3600*2-1 {
			t_ := int64(1000) * int64(k) + int64(rand.Float64()*float64(devMag))
			app := h.Appender()
			s := scanner.Text()
			data := strings.Split(s, ",")
			for i := 0; i < len(lsets); i += 101 {
				tempData := make([]float64, 0, 101)
				for j := i; j < i + 101; j++ {
					v, _ := strconv.ParseFloat(data[j], 64)
					tempData = append(tempData, v)
				}
				if refs[i/101] == 0 {
					ref, err := app.AddGroup(lsets[i:i+101], t_, tempData)
					testutil.Ok(t, err)
					refs[i/101] = ref
				} else {
					err := app.AddGroupFast(refs[i/101], t_, tempData)
					testutil.Ok(t, err)
				}
			}
			totalSamples += len(lsets)
			err := app.Commit()
			testutil.Ok(t, err)

			k += 1
		}
		fmt.Println("total appended samples", totalSamples)

		t.Run("Group Head Timeseries 50s interval", func(t *testing.T) {
			starts := []int64{}
			ends := []int64{}
			for i := 0; i < 3600*2; i += 50 {
				starts = append(starts, int64(i * 1000))
				ends = append(ends, int64((i + 50) * 1000))
			}

			// Prepare matchers.
			matchers := []labels.Matcher{}
			for _, l := range queryLabels {
				matchers = append(matchers, labels.NewEqualMatcher(l.Name, l.Value))
			}

			totalSamples = 0
			start := time.Now()
			for i := 0; i < len(starts); i++ {
				q, err := NewBlockQuerier(h, starts[i], ends[i])
				testutil.Ok(t, err)

				for _, matcher := range matchers {
					ss, err := q.Select(matcher)
					testutil.Ok(t, err)

					for ss.Next() {
						it := ss.At().Iterator()
						for it.Next() {
							totalSamples += 1
						}
					}
				}
				q.Close()
			}
			fmt.Println("total query samples", totalSamples)
			fmt.Printf("Group head timeseries 100s interval duration=%f\n", time.Since(start).Seconds())
		})
		t.Run("Group Head Timeseries 100s interval", func(t *testing.T) {
			starts := []int64{}
			ends := []int64{}
			for i := 0; i < 3600*2; i += 100 {
				starts = append(starts, int64(i * 1000))
				ends = append(ends, int64((i + 100) * 1000))
			}

			// Prepare matchers.
			matchers := []labels.Matcher{}
			for _, l := range queryLabels {
				matchers = append(matchers, labels.NewEqualMatcher(l.Name, l.Value))
			}

			totalSamples = 0
			start := time.Now()
			for i := 0; i < len(starts); i++ {
				q, err := NewBlockQuerier(h, starts[i], ends[i])
				testutil.Ok(t, err)

				for _, matcher := range matchers {
					ss, err := q.Select(matcher)
					testutil.Ok(t, err)

					for ss.Next() {
						it := ss.At().Iterator()
						for it.Next() {
							totalSamples += 1
						}
					}
				}
				q.Close()
			}
			fmt.Println("total query samples", totalSamples)
			fmt.Printf("Group head timeseries 100s interval duration=%f\n", time.Since(start).Seconds())
		})
		t.Run("Group Head Timeseries 300s interval", func(t *testing.T) {
			starts := []int64{}
			ends := []int64{}
			for i := 0; i < 3600*2; i += 300 {
				starts = append(starts, int64(i * 1000))
				ends = append(ends, int64((i + 300) * 1000))
			}

			// Prepare matchers.
			matchers := []labels.Matcher{}
			for _, l := range queryLabels {
				matchers = append(matchers, labels.NewEqualMatcher(l.Name, l.Value))
			}

			totalSamples = 0
			start := time.Now()
			for i := 0; i < len(starts); i++ {
				q, err := NewBlockQuerier(h, starts[i], ends[i])
				testutil.Ok(t, err)

				for _, matcher := range matchers {
					ss, err := q.Select(matcher)
					testutil.Ok(t, err)

					for ss.Next() {
						it := ss.At().Iterator()
						for it.Next() {
							totalSamples += 1
						}
					}
				}
				q.Close()
			}
			fmt.Println("total query samples", totalSamples)
			fmt.Printf("Group head timeseries 300s interval duration=%f\n", time.Since(start).Seconds())
		})
		t.Run("Group Head Timeseries 500s interval", func(t *testing.T) {
			starts := []int64{}
			ends := []int64{}
			for i := 0; i < 3600*2; i += 500 {
				starts = append(starts, int64(i * 1000))
				ends = append(ends, int64((i + 500) * 1000))
			}

			// Prepare matchers.
			matchers := []labels.Matcher{}
			for _, l := range queryLabels {
				matchers = append(matchers, labels.NewEqualMatcher(l.Name, l.Value))
			}

			totalSamples = 0
			start := time.Now()
			for i := 0; i < len(starts); i++ {
				q, err := NewBlockQuerier(h, starts[i], ends[i])
				testutil.Ok(t, err)

				for _, matcher := range matchers {
					ss, err := q.Select(matcher)
					testutil.Ok(t, err)

					for ss.Next() {
						it := ss.At().Iterator()
						for it.Next() {
							totalSamples += 1
						}
					}
				}
				q.Close()
			}
			fmt.Println("total query samples", totalSamples)
			fmt.Printf("Group head timeseries 500s interval duration=%f\n", time.Since(start).Seconds())
		})
	}
}

// func BenchmarkHeadStripeSeriesCreate(b *testing.B) {
// 	// Put a series, select it. GC it and then access it.
// 	h, err := NewHead(nil, nil, nil, 1000)
// 	testutil.Ok(b, err)
// 	defer h.Close()

// 	for i := 0; i < b.N; i++ {
// 		h.getOrCreate(uint64(i), labels.FromStrings("a", strconv.Itoa(i)))
// 	}
// }

// func BenchmarkHeadStripeSeriesCreateParallel(b *testing.B) {
// 	// Put a series, select it. GC it and then access it.
// 	h, err := NewHead(nil, nil, nil, 1000)
// 	testutil.Ok(b, err)
// 	defer h.Close()

// 	var count int64

// 	b.RunParallel(func(pb *testing.PB) {
// 		for pb.Next() {
// 			i := atomic.AddInt64(&count, 1)
// 			h.getOrCreate(uint64(i), labels.FromStrings("a", strconv.Itoa(int(i))))
// 		}
// 	})
// }

// func BenchmarkHeadPostingForMatchers(b *testing.B) {
// 	h, err := NewHead(nil, nil, nil, 1000)
// 	testutil.Ok(b, err)
// 	defer func() {
// 		testutil.Ok(b, h.Close())
// 	}()

// 	var ref uint64

// 	addSeries := func(l labels.Labels) {
// 		ref++
// 		h.getOrCreateWithID(ref, l.Hash(), l)
// 	}

// 	for n := 0; n < 10; n++ {
// 		for i := 0; i < 100000; i++ {
// 			addSeries(labels.FromStrings("i", strconv.Itoa(i), "n", strconv.Itoa(n), "j", "foo"))
// 			// Have some series that won't be matched, to properly test inverted matches.
// 			addSeries(labels.FromStrings("i", strconv.Itoa(i), "n", strconv.Itoa(n), "j", "bar"))
// 			addSeries(labels.FromStrings("i", strconv.Itoa(i), "n", "0_"+strconv.Itoa(n), "j", "bar"))
// 			addSeries(labels.FromStrings("i", strconv.Itoa(i), "n", "1_"+strconv.Itoa(n), "j", "bar"))
// 			addSeries(labels.FromStrings("i", strconv.Itoa(i), "n", "2_"+strconv.Itoa(n), "j", "foo"))
// 		}
// 	}

// 	n1 := labels.NewEqualMatcher("n", "1")

// 	jFoo := labels.NewEqualMatcher("j", "foo")
// 	jNotFoo := labels.Not(jFoo)

// 	iStar := labels.NewMustRegexpMatcher("i", "^.*$")
// 	iPlus := labels.NewMustRegexpMatcher("i", "^.+$")
// 	i1Plus := labels.NewMustRegexpMatcher("i", "^1.+$")
// 	iEmptyRe := labels.NewMustRegexpMatcher("i", "^$")
// 	iNotEmpty := labels.Not(labels.NewEqualMatcher("i", ""))
// 	iNot2 := labels.Not(labels.NewEqualMatcher("n", "2"))
// 	iNot2Star := labels.Not(labels.NewMustRegexpMatcher("i", "^2.*$"))

// 	cases := []struct {
// 		name     string
// 		matchers []labels.Matcher
// 	}{
// 		{`n="1"`, []labels.Matcher{n1}},
// 		{`n="1",j="foo"`, []labels.Matcher{n1, jFoo}},
// 		{`j="foo",n="1"`, []labels.Matcher{jFoo, n1}},
// 		{`n="1",j!="foo"`, []labels.Matcher{n1, jNotFoo}},
// 		{`i=~".*"`, []labels.Matcher{iStar}},
// 		{`i=~".+"`, []labels.Matcher{iPlus}},
// 		{`i=~""`, []labels.Matcher{iEmptyRe}},
// 		{`i!=""`, []labels.Matcher{iNotEmpty}},
// 		{`n="1",i=~".*",j="foo"`, []labels.Matcher{n1, iStar, jFoo}},
// 		{`n="1",i=~".*",i!="2",j="foo"`, []labels.Matcher{n1, iStar, iNot2, jFoo}},
// 		{`n="1",i!=""`, []labels.Matcher{n1, iNotEmpty}},
// 		{`n="1",i!="",j="foo"`, []labels.Matcher{n1, iNotEmpty, jFoo}},
// 		{`n="1",i=~".+",j="foo"`, []labels.Matcher{n1, iPlus, jFoo}},
// 		{`n="1",i=~"1.+",j="foo"`, []labels.Matcher{n1, i1Plus, jFoo}},
// 		{`n="1",i=~".+",i!="2",j="foo"`, []labels.Matcher{n1, iPlus, iNot2, jFoo}},
// 		{`n="1",i=~".+",i!~"2.*",j="foo"`, []labels.Matcher{n1, iPlus, iNot2Star, jFoo}},
// 	}

// 	for _, c := range cases {
// 		b.Run(c.name, func(b *testing.B) {
// 			for i := 0; i < b.N; i++ {
// 				_, err := PostingsForMatchers(h.indexRange(0, 1000), c.matchers...)
// 				testutil.Ok(b, err)
// 			}
// 		})
// 	}
// }
