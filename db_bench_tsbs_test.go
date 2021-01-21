package tsdb

import (
	// "flag"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/naivewong/tsdb-group/labels"
	"github.com/naivewong/tsdb-group/testutil"
	tsdb_origin "github.com/prometheus/tsdb"
	origin_labels "github.com/prometheus/tsdb/labels"
)

var queryRange = int64(300000)
var numSeries = 5050
var originSleep = time.Duration(30)
var groupSleep = time.Duration(60)

// func init() {
// 	flag.Int64Var(&queryRange, "qr", 300000, "query range (ms)")
// 	flag.IntVar(&numSeries, "num", 5050, "num of time series")
// 	flag.DurationVar(&originSleep, "os", 20*time.Second, "original tsdb sleep")
// 	flag.DurationVar(&groupSleep, "gs", 60*time.Second, "group tsdb sleep")
// }

func BenchmarkDBtsbs(b *testing.B) {
	fmt.Println("-------- Benchmark DB tsbs ---------")

	timeDeltas := []int64{5000, 10000, 15000, 30000}
	// timeDeltas := []int64{15000}
	for _, timeDelta := range timeDeltas {
		fmt.Println("------- time delta", timeDelta)
		var (
			devMag             = int64(100)
			scrapeCount        = 3600*12*1000/int(timeDelta)
			numQueryLabels     = 200
			someMatchers       = []origin_labels.Matcher{
				origin_labels.NewEqualMatcher("__name__", "cpu_usage_user"),
				origin_labels.NewEqualMatcher("__name__", "diskio_reads"),
				origin_labels.NewEqualMatcher("__name__", "kernel_boot_time"),
				origin_labels.NewEqualMatcher("__name__", "mem_total"),
				origin_labels.NewEqualMatcher("__name__", "net_bytes_sent"),
			}
			cpuMatchers        = []origin_labels.Matcher{
				origin_labels.NewEqualMatcher("__name__", "cpu_usage_user"),
				origin_labels.NewEqualMatcher("__name__", "cpu_usage_system"),
				origin_labels.NewEqualMatcher("__name__", "cpu_usage_idle"),
				origin_labels.NewEqualMatcher("__name__", "cpu_usage_nice"),
				origin_labels.NewEqualMatcher("__name__", "cpu_usage_iowait"),
			}
			cpuMatcher, _      = origin_labels.NewRegexpMatcher("__name__", "cpu_.*")
			memstatsMatchers   = []origin_labels.Matcher{
				origin_labels.NewEqualMatcher("__name__", "go_memstats_alloc_bytes"),
				origin_labels.NewEqualMatcher("__name__", "go_memstats_alloc_bytes_total"),
				origin_labels.NewEqualMatcher("__name__", "go_memstats_buck_hash_sys_bytes"),
				origin_labels.NewEqualMatcher("__name__", "go_memstats_frees_total"),
				origin_labels.NewEqualMatcher("__name__", "go_memstats_gc_cpu_fraction"),
				origin_labels.NewEqualMatcher("__name__", "go_memstats_gc_sys_bytes"),
				origin_labels.NewEqualMatcher("__name__", "go_memstats_heap_alloc_bytes"),
				origin_labels.NewEqualMatcher("__name__", "go_memstats_heap_idle_bytes"),
				origin_labels.NewEqualMatcher("__name__", "go_memstats_heap_inuse_bytes"),
				origin_labels.NewEqualMatcher("__name__", "go_memstats_heap_objects"),
				origin_labels.NewEqualMatcher("__name__", "go_memstats_heap_released_bytes"),
				origin_labels.NewEqualMatcher("__name__", "go_memstats_heap_sys_bytes"),
				origin_labels.NewEqualMatcher("__name__", "go_memstats_last_gc_time_seconds"),
				origin_labels.NewEqualMatcher("__name__", "go_memstats_lookups_total"),
				origin_labels.NewEqualMatcher("__name__", "go_memstats_mallocs_total"),
				origin_labels.NewEqualMatcher("__name__", "go_memstats_mcache_inuse_bytes"),
				origin_labels.NewEqualMatcher("__name__", "go_memstats_mcache_sys_bytes"),
				origin_labels.NewEqualMatcher("__name__", "go_memstats_mspan_inuse_bytes"),
				origin_labels.NewEqualMatcher("__name__", "go_memstats_mspan_sys_bytes"),
				origin_labels.NewEqualMatcher("__name__", "go_memstats_next_gc_bytes"),
				origin_labels.NewEqualMatcher("__name__", "go_memstats_other_sys_bytes"),
				origin_labels.NewEqualMatcher("__name__", "go_memstats_stack_inuse_bytes"),
				origin_labels.NewEqualMatcher("__name__", "go_memstats_stack_sys_bytes"),
				origin_labels.NewEqualMatcher("__name__", "go_memstats_sys_bytes"),
			}
			memstatsMatcher, _ = origin_labels.NewRegexpMatcher("__name__", "go_memstats_.*")
		)
		fmt.Println(cpuMatchers, cpuMatcher, memstatsMatcher)
		{
			dbPath := "tsbs_bench/db_bench_random/" + strconv.Itoa(int(timeDelta))
			os.RemoveAll(dbPath)
			logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
			db, err := tsdb_origin.Open(dbPath, logger, nil, &tsdb_origin.Options{
				WALSegmentSize:         -1,
				RetentionDuration:      15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds
				BlockRanges:            ExponentialBlockRanges(int64(2*time.Hour)/1e6, 3, 5),
				NoLockfile:             false,
				AllowOverlappingBlocks: false,
			})
			testutil.Ok(b, err)

			lsets := []origin_labels.Labels{}
			queryLabels := []origin_labels.Label{}

			LoadDevOpsSeries(b, &lsets, &queryLabels, numSeries, numQueryLabels, true)
			DBAppendRandom(b, db, timeDelta, devMag, scrapeCount, lsets)
			time.Sleep(time.Second*originSleep)

			starts := []int64{}
			ends := []int64{}
			for i := int64(0); i < 3600*12*1000/timeDelta; i += queryRange/timeDelta {
				starts = append(starts, i * timeDelta)
				ends = append(ends, (i + queryRange / timeDelta) * timeDelta)
			}
			totalSamples := 0

			// Prepare matchers.
			matchers := []origin_labels.Matcher{}
			for i := 0; i < 50; i++ {
				matchers = append(matchers, origin_labels.NewEqualMatcher("hostname", "host_"+strconv.Itoa(i)))
			}

			b.Run("BenchmarkDBSingleGroupBy1-1-1 random"+strconv.Itoa(int(timeDelta)), func(b *testing.B) {
				b.ResetTimer()
				for iter := 0; iter < b.N; iter++ {
					for i := len(starts)/12*11; i < len(starts); i++ {
						q, err := db.Querier(starts[i], ends[i])
						testutil.Ok(b, err)

						ss, err := q.Select(matchers[iter%50], someMatchers[0])
						testutil.Ok(b, err)

						for ss.Next() {
							it := ss.At().Iterator()
							for it.Next() {
								totalSamples += 1
							}
						}
						q.Close()
					}
				}
			})

			// Simple aggregrate (MAX) on one metric for 1 host, every 5 mins for 12 hours.
			b.Run("BenchmarkDBSingleGroupBy1-1-12 random"+strconv.Itoa(int(timeDelta)), func(b *testing.B) {
				b.ResetTimer()
				for iter := 0; iter < b.N; iter++ {
					for i := 0; i < len(starts); i++ {
						q, err := db.Querier(starts[i], ends[i])
						testutil.Ok(b, err)

						ss, err := q.Select(matchers[iter%50], someMatchers[0])
						testutil.Ok(b, err)

						for ss.Next() {
							it := ss.At().Iterator()
							for it.Next() {
								totalSamples += 1
							}
						}
						q.Close()
					}
				}
			})

			// Simple aggregrate (MAX) on one metric for 8 hosts, every 5 mins for 1 hour.
			b.Run("BenchmarkDBSingleGroupBy1-8-1 random"+strconv.Itoa(int(timeDelta)), func(b *testing.B) {
				b.ResetTimer()
				count := 0
				for iter := 0; iter < b.N; iter++ {
					for i := len(starts)/12*11; i < len(starts); i++ {
						q, err := db.Querier(starts[i], ends[i])
						testutil.Ok(b, err)

						for j := 0; j < 8; j++ {
							ss, err := q.Select(matchers[count%50], someMatchers[0])
							testutil.Ok(b, err)

							for ss.Next() {
								it := ss.At().Iterator()
								for it.Next() {
									totalSamples += 1
								}
							}
							count++
						}
						q.Close()
					}
				}
			})

			// Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 1 hour.
			b.Run("BenchmarkDBSingleGroupBy5-1-1 random"+strconv.Itoa(int(timeDelta)), func(b *testing.B) {
				b.ResetTimer()
				for iter := 0; iter < b.N; iter++ {
					for i := len(starts)/12*11; i < len(starts); i++ {
						q, err := db.Querier(starts[i], ends[i])
						testutil.Ok(b, err)

						for j := 0; j < 5; j++ {
							ss, err := q.Select(matchers[iter%50], someMatchers[j])
							testutil.Ok(b, err)

							for ss.Next() {
								it := ss.At().Iterator()
								for it.Next() {
									totalSamples += 1
								}
							}
						}
						q.Close()
					}
				}
			})

			// Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 12 hour.
			b.Run("BenchmarkDBSingleGroupBy5-1-12 random"+strconv.Itoa(int(timeDelta)), func(b *testing.B) {
				b.ResetTimer()
				for iter := 0; iter < b.N; iter++ {
					for i := 0; i < len(starts); i++ {
						q, err := db.Querier(starts[i], ends[i])
						testutil.Ok(b, err)

						for j := 0; j < 5; j++ {
							ss, err := q.Select(matchers[iter%50], someMatchers[j])
							testutil.Ok(b, err)

							for ss.Next() {
								it := ss.At().Iterator()
								for it.Next() {
									totalSamples += 1
								}
							}
						}
						q.Close()
					}
				}
			})

			// Simple aggregrate (MAX) on 5 metrics for 8 hosts, every 5 mins for 1 hour.
			b.Run("BenchmarkDBSingleGroupBy5-8-1 random"+strconv.Itoa(int(timeDelta)), func(b *testing.B) {
				b.ResetTimer()
				count := 0
				for iter := 0; iter < b.N; iter++ {
					for i := len(starts)/12*11; i < len(starts); i++ {
						q, err := db.Querier(starts[i], ends[i])
						testutil.Ok(b, err)

						for k := 0; k < 8; k++ {
							for j := 0; j < 5; j++ {
								ss, err := q.Select(matchers[count%50], someMatchers[j])
								testutil.Ok(b, err)

								for ss.Next() {
									it := ss.At().Iterator()
									for it.Next() {
										totalSamples += 1
									}
								}
							}
							count++
						}
						q.Close()
					}
				}
			})

			db.Close()
		}
		{
			dbPath := "tsbs_bench/db_bench_ne_timeseries/" + strconv.Itoa(int(timeDelta))
			os.RemoveAll(dbPath)
			logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
			db, err := tsdb_origin.Open(dbPath, logger, nil, &tsdb_origin.Options{
				WALSegmentSize:         -1,
				RetentionDuration:      15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds
				BlockRanges:            ExponentialBlockRanges(int64(2*time.Hour)/1e6, 3, 5),
				NoLockfile:             false,
				AllowOverlappingBlocks: false,
			})
			testutil.Ok(b, err)

			lsets := []origin_labels.Labels{}
			queryLabels := []origin_labels.Label{}

			LoadNodeExporterSeries(b, &lsets, &queryLabels, 585, numQueryLabels, numSeries, true)
			DBAppendNodeExporterTimeseries(b, db, timeDelta, devMag, lsets)

			time.Sleep(time.Second*originSleep)

			starts := []int64{}
			ends := []int64{}
			for i := int64(0); i < 3600*12*1000/timeDelta; i += queryRange/timeDelta {
				starts = append(starts, i * timeDelta)
				ends = append(ends, (i + queryRange / timeDelta) * timeDelta)
			}
			totalSamples := 0

			// Prepare matchers.
			matchers := []origin_labels.Matcher{}
			for i := 0; i < 10; i++ {
				matchers = append(matchers, origin_labels.NewEqualMatcher("instance", fmt.Sprintf("pc9%06d:9100", i)))
			}

			b.Run("BenchmarkDBSingleGroupBy1-1-1 timeseries"+strconv.Itoa(int(timeDelta)), func(b *testing.B) {
				b.ResetTimer()
				for iter := 0; iter < b.N; iter++ {
					for i := len(starts)/12*11; i < len(starts); i++ {
						q, err := db.Querier(starts[i], ends[i])
						testutil.Ok(b, err)

						ss, err := q.Select(matchers[iter%10], memstatsMatchers[4])
						testutil.Ok(b, err)

						for ss.Next() {
							it := ss.At().Iterator()
							for it.Next() {
								totalSamples += 1
							}
						}
						q.Close()
					}
				}
			})

			// Simple aggregrate (MAX) on one metric for 1 host, every 5 mins for 12 hours.			
			b.Run("BenchmarkDBSingleGroupBy1-1-12 timeseries"+strconv.Itoa(int(timeDelta)), func(b *testing.B) {
				b.ResetTimer()
				for iter := 0; iter < b.N; iter++ {
					for i := 0; i < len(starts); i++ {
						q, err := db.Querier(starts[i], ends[i])
						testutil.Ok(b, err)

						ss, err := q.Select(matchers[iter%10], memstatsMatchers[4])
						testutil.Ok(b, err)

						for ss.Next() {
							it := ss.At().Iterator()
							for it.Next() {
								totalSamples += 1
							}
						}
						q.Close()
					}
				}
			})

			// Simple aggregrate (MAX) on one metric for 8 hosts, every 5 mins for 1 hour.			
			b.Run("BenchmarkDBSingleGroupBy1-8-1 timeseries"+strconv.Itoa(int(timeDelta)), func(b *testing.B) {
				b.ResetTimer()
				count := 0
				for iter := 0; iter < b.N; iter++ {
					for i := len(starts)/12*11; i < len(starts); i++ {
						q, err := db.Querier(starts[i], ends[i])
						testutil.Ok(b, err)

						for j := 0; j < 8; j++ {
							ss, err := q.Select(matchers[count%10], memstatsMatchers[4])
							testutil.Ok(b, err)

							for ss.Next() {
								it := ss.At().Iterator()
								for it.Next() {
									totalSamples += 1
								}
							}
							count++
						}
						q.Close()
					}
				}
			})

			// Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 1 hour.
			b.Run("BenchmarkDBSingleGroupBy5-1-1 timeseries"+strconv.Itoa(int(timeDelta)), func(b *testing.B) {
				b.ResetTimer()
				for iter := 0; iter < b.N; iter++ {
					for i := len(starts)/12*11; i < len(starts); i++ {
						q, err := db.Querier(starts[i], ends[i])
						testutil.Ok(b, err)

						for j := 4; j < 9; j++ {
							ss, err := q.Select(matchers[iter%10], memstatsMatchers[j])
							testutil.Ok(b, err)

							for ss.Next() {
								it := ss.At().Iterator()
								for it.Next() {
									totalSamples += 1
								}
							}
						}
						q.Close()
					}
				}
			})

			// Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 12 hour.
			b.Run("BenchmarkDBSingleGroupBy5-1-12 timeseries"+strconv.Itoa(int(timeDelta)), func(b *testing.B) {
				b.ResetTimer()
				for iter := 0; iter < b.N; iter++ {
					for i := 0; i < len(starts); i++ {
						q, err := db.Querier(starts[i], ends[i])
						testutil.Ok(b, err)

						for j := 4; j < 9; j++ {
							ss, err := q.Select(matchers[iter%10], memstatsMatchers[j])
							testutil.Ok(b, err)

							for ss.Next() {
								it := ss.At().Iterator()
								for it.Next() {
									totalSamples += 1
								}
							}
						}
						q.Close()
					}
				}
			})

			// Simple aggregrate (MAX) on 5 metrics for 8 hosts, every 5 mins for 1 hour.
			b.Run("BenchmarkDBSingleGroupBy5-8-1 timeseries"+strconv.Itoa(int(timeDelta)), func(b *testing.B) {
				b.ResetTimer()
				count := 0
				for iter := 0; iter < b.N; iter++ {
					for i := len(starts)/12*11; i < len(starts); i++ {
						q, err := db.Querier(starts[i], ends[i])
						testutil.Ok(b, err)

						for k := 0; k < 8; k++ {
							for j := 4; j < 9; j++ {
								ss, err := q.Select(matchers[count%10], memstatsMatchers[j])
								testutil.Ok(b, err)

								for ss.Next() {
									it := ss.At().Iterator()
									for it.Next() {
										totalSamples += 1
									}
								}
							}
							count++
						}
						q.Close()
					}
				}
			})

			db.Close()
		}
	}
}

func BenchmarkGroupDBtsbs(b *testing.B) {
	fmt.Println("-------- Benchmark GroupDB tsbs ---------")

	timeDeltas := []int64{5000, 10000, 15000, 30000}
	// timeDeltas := []int64{15000}
	for _, timeDelta := range timeDeltas {
		fmt.Println("------- time delta", timeDelta)
		var (
			devMag             = int64(100)
			scrapeCount        = 3600*12*1000/int(timeDelta)
			numQueryLabels     = 200
			seriesPerGroup     = 101
			someMatchers       = []labels.Matcher{
				labels.NewEqualMatcher("__name__", "cpu_usage_user"),
				labels.NewEqualMatcher("__name__", "diskio_reads"),
				labels.NewEqualMatcher("__name__", "kernel_boot_time"),
				labels.NewEqualMatcher("__name__", "mem_total"),
				labels.NewEqualMatcher("__name__", "net_bytes_sent"),
			}
			cpuMatchers        = []labels.Matcher{
				labels.NewEqualMatcher("__name__", "cpu_usage_user"),
				labels.NewEqualMatcher("__name__", "cpu_usage_system"),
				labels.NewEqualMatcher("__name__", "cpu_usage_idle"),
				labels.NewEqualMatcher("__name__", "cpu_usage_nice"),
				labels.NewEqualMatcher("__name__", "cpu_usage_iowait"),
			}
			cpuMatcher, _      = labels.NewRegexpMatcher("__name__", "cpu_.*")
			memstatsMatchers   = []labels.Matcher{
				labels.NewEqualMatcher("__name__", "go_memstats_alloc_bytes"),
				labels.NewEqualMatcher("__name__", "go_memstats_alloc_bytes_total"),
				labels.NewEqualMatcher("__name__", "go_memstats_buck_hash_sys_bytes"),
				labels.NewEqualMatcher("__name__", "go_memstats_frees_total"),
				labels.NewEqualMatcher("__name__", "go_memstats_gc_cpu_fraction"),
				labels.NewEqualMatcher("__name__", "go_memstats_gc_sys_bytes"),
				labels.NewEqualMatcher("__name__", "go_memstats_heap_alloc_bytes"),
				labels.NewEqualMatcher("__name__", "go_memstats_heap_idle_bytes"),
				labels.NewEqualMatcher("__name__", "go_memstats_heap_inuse_bytes"),
				labels.NewEqualMatcher("__name__", "go_memstats_heap_objects"),
				labels.NewEqualMatcher("__name__", "go_memstats_heap_released_bytes"),
				labels.NewEqualMatcher("__name__", "go_memstats_heap_sys_bytes"),
				labels.NewEqualMatcher("__name__", "go_memstats_last_gc_time_seconds"),
				labels.NewEqualMatcher("__name__", "go_memstats_lookups_total"),
				labels.NewEqualMatcher("__name__", "go_memstats_mallocs_total"),
				labels.NewEqualMatcher("__name__", "go_memstats_mcache_inuse_bytes"),
				labels.NewEqualMatcher("__name__", "go_memstats_mcache_sys_bytes"),
				labels.NewEqualMatcher("__name__", "go_memstats_mspan_inuse_bytes"),
				labels.NewEqualMatcher("__name__", "go_memstats_mspan_sys_bytes"),
				labels.NewEqualMatcher("__name__", "go_memstats_next_gc_bytes"),
				labels.NewEqualMatcher("__name__", "go_memstats_other_sys_bytes"),
				labels.NewEqualMatcher("__name__", "go_memstats_stack_inuse_bytes"),
				labels.NewEqualMatcher("__name__", "go_memstats_stack_sys_bytes"),
				labels.NewEqualMatcher("__name__", "go_memstats_sys_bytes"),
			}
			memstatsMatcher, _ = labels.NewRegexpMatcher("__name__", "go_memstats_.*")
		)
		fmt.Println(cpuMatchers, cpuMatcher, memstatsMatcher)
		{
			dbPath := "tsbs_bench/group_db_bench_random/" + strconv.Itoa(int(timeDelta))
			os.RemoveAll(dbPath)
			logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
			db, err := Open(dbPath, logger, nil, &Options{
				WALSegmentSize:         -1,
				RetentionDuration:      15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds
				BlockRanges:            ExponentialBlockRanges(int64(2*time.Hour)/1e6, 3, 5),
				NoLockfile:             false,
				AllowOverlappingBlocks: false,
				WALCompression:         false,
			})
			testutil.Ok(b, err)

			lsets := []labels.Labels{}
			queryLabels := []labels.Label{}

			GroupLoadDevOpsSeries(b, &lsets, &queryLabels, seriesPerGroup, numSeries, numQueryLabels, true)
			GroupDBAppendRandom(b, db, timeDelta, devMag, seriesPerGroup, scrapeCount, lsets)
			time.Sleep(time.Second*groupSleep)

			starts := []int64{}
			ends := []int64{}
			for i := int64(0); i < 3600*12*1000/timeDelta; i += queryRange/timeDelta {
				starts = append(starts, i * timeDelta)
				ends = append(ends, (i + queryRange / timeDelta) * timeDelta)
			}
			totalSamples := 0

			// Prepare matchers.
			matchers := []labels.Matcher{}
			for i := 0; i < 50; i++ {
				matchers = append(matchers, labels.NewEqualMatcher("hostname", "host_"+strconv.Itoa(i)))
			}

			b.Run("BenchmarkGroupDBSingleGroupBy1-1-1 random"+strconv.Itoa(int(timeDelta)), func(b *testing.B) {
				b.ResetTimer()
				for iter := 0; iter < b.N; iter++ {
					for i := len(starts)/12*11; i < len(starts); i++ {
						q, err := db.Querier(starts[i], ends[i])
						testutil.Ok(b, err)

						ss, err := q.Select(matchers[iter%50], someMatchers[0])
						testutil.Ok(b, err)

						for ss.Next() {
							it := ss.At().Iterator()
							for it.Next() {
								totalSamples += 1
							}
						}
						q.Close()
					}
				}
			})

			// Simple aggregrate (MAX) on one metric for 1 host, every 5 mins for 12 hours.
			b.Run("BenchmarkGroupDBSingleGroupBy1-1-12 random"+strconv.Itoa(int(timeDelta)), func(b *testing.B) {
				b.ResetTimer()
				for iter := 0; iter < b.N; iter++ {
					for i := 0; i < len(starts); i++ {
						q, err := db.Querier(starts[i], ends[i])
						testutil.Ok(b, err)

						ss, err := q.Select(matchers[iter%50], someMatchers[0])
						testutil.Ok(b, err)

						for ss.Next() {
							it := ss.At().Iterator()
							for it.Next() {
								totalSamples += 1
							}
						}
						q.Close()
					}
				}
			})

			// Simple aggregrate (MAX) on 5 metrics for 8 hosts, every 5 mins for 1 hour.
			b.Run("BenchmarkGroupDBSingleGroupBy1-8-1 random"+strconv.Itoa(int(timeDelta)), func(b *testing.B) {
				b.ResetTimer()
				count := 0
				for iter := 0; iter < b.N; iter++ {
					for i := len(starts)/12*11; i < len(starts); i++ {
						q, err := db.Querier(starts[i], ends[i])
						testutil.Ok(b, err)

						for j := 0; j < 8; j++ {
							ss, err := q.Select(matchers[count%50], someMatchers[0])
							testutil.Ok(b, err)

							for ss.Next() {
								it := ss.At().Iterator()
								for it.Next() {
									totalSamples += 1
								}
							}
							count++
						}
						q.Close()
					}
				}
			})

			// Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 1 hour.
			b.Run("BenchmarkGroupDBSingleGroupBy5-1-1 random"+strconv.Itoa(int(timeDelta)), func(b *testing.B) {
				b.ResetTimer()
				for iter := 0; iter < b.N; iter++ {
					for i := len(starts)/12*11; i < len(starts); i++ {
						q, err := db.Querier(starts[i], ends[i])
						testutil.Ok(b, err)

						for j := 0; j < 5; j++ {
							ss, err := q.Select(matchers[iter%50], someMatchers[j])
							testutil.Ok(b, err)

							for ss.Next() {
								it := ss.At().Iterator()
								for it.Next() {
									totalSamples += 1
								}
							}
						}
						q.Close()
					}
				}
			})

			// Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 12 hour.
			b.Run("BenchmarkGroupDBSingleGroupBy5-1-12 random"+strconv.Itoa(int(timeDelta)), func(b *testing.B) {
				b.ResetTimer()
				for iter := 0; iter < b.N; iter++ {
					for i := 0; i < len(starts); i++ {
						q, err := db.Querier(starts[i], ends[i])
						testutil.Ok(b, err)

						for j := 0; j < 5; j++ {
							ss, err := q.Select(matchers[iter%50], someMatchers[j])
							testutil.Ok(b, err)

							for ss.Next() {
								it := ss.At().Iterator()
								for it.Next() {
									totalSamples += 1
								}
							}
						}
						q.Close()
					}
				}
			})

			// Simple aggregrate (MAX) on 5 metrics for 8 hosts, every 5 mins for 1 hour.
			b.Run("BenchmarkGroupDBSingleGroupBy5-8-1 random"+strconv.Itoa(int(timeDelta)), func(b *testing.B) {
				b.ResetTimer()
				count := 0
				for iter := 0; iter < b.N; iter++ {
					for i := len(starts)/12*11; i < len(starts); i++ {
						q, err := db.Querier(starts[i], ends[i])
						testutil.Ok(b, err)

						for k := 0; k < 8; k++ {
							for j := 0; j < 5; j++ {
								ss, err := q.Select(matchers[count%50], someMatchers[j])
								testutil.Ok(b, err)

								for ss.Next() {
									it := ss.At().Iterator()
									for it.Next() {
										totalSamples += 1
									}
								}
							}
							count++
						}
						q.Close()
					}
				}
			})

			db.Close()
		}
		{
			dbPath := "tsbs_bench/group_db_bench_timeseries/" + strconv.Itoa(int(timeDelta))
			os.RemoveAll(dbPath)
			logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
			db, err := Open(dbPath, logger, nil, &Options{
				WALSegmentSize:         -1,
				RetentionDuration:      15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds
				BlockRanges:            ExponentialBlockRanges(int64(2*time.Hour)/1e6, 3, 5),
				NoLockfile:             false,
				AllowOverlappingBlocks: false,
				WALCompression:         false,
			})
			testutil.Ok(b, err)

			lsets := []labels.Labels{}
			queryLabels := []labels.Label{}

			GroupLoadNodeExporterSeries(b, &lsets, &queryLabels, seriesPerGroup, 585, numQueryLabels, numSeries, true)
			GroupDBAppendNodeExporterTimeseries(b, db, timeDelta, devMag, seriesPerGroup, lsets)

			time.Sleep(time.Second*groupSleep)

			starts := []int64{}
			ends := []int64{}
			for i := int64(0); i < 3600*12*1000/timeDelta; i += queryRange/timeDelta {
				starts = append(starts, i * timeDelta)
				ends = append(ends, (i + queryRange / timeDelta) * timeDelta)
			}
			totalSamples := 0

			// Prepare matchers.
			matchers := []labels.Matcher{}
			for i := 0; i < 10; i++ {
				matchers = append(matchers, labels.NewEqualMatcher("instance", fmt.Sprintf("pc9%06d:9100", i)))
			}

			b.Run("BenchmarkGroupDBSingleGroupBy1-1-1 timeseries"+strconv.Itoa(int(timeDelta)), func(b *testing.B) {
				b.ResetTimer()
				for iter := 0; iter < b.N; iter++ {
					for i := len(starts)/12*11; i < len(starts); i++ {
						q, err := db.Querier(starts[i], ends[i])
						testutil.Ok(b, err)

						ss, err := q.Select(matchers[iter%10], memstatsMatchers[4])
						testutil.Ok(b, err)

						for ss.Next() {
							it := ss.At().Iterator()
							for it.Next() {
								totalSamples += 1
							}
						}

						q.Close()
					}
				}
			})

			// Simple aggregrate (MAX) on one metric for 1 host, every 5 mins for 12 hours.			
			b.Run("BenchmarkGroupDBSingleGroupBy1-1-12 timeseries"+strconv.Itoa(int(timeDelta)), func(b *testing.B) {
				b.ResetTimer()
				for iter := 0; iter < b.N; iter++ {
					for i := 0; i < len(starts); i++ {
						q, err := db.Querier(starts[i], ends[i])
						testutil.Ok(b, err)

						ss, err := q.Select(matchers[iter%10], memstatsMatchers[4])
						testutil.Ok(b, err)

						for ss.Next() {
							it := ss.At().Iterator()
							for it.Next() {
								totalSamples += 1
							}
						}
						q.Close()
					}
				}
			})

			// Simple aggregrate (MAX) on one metric for 8 hosts, every 5 mins for 1 hour.			
			b.Run("BenchmarkGroupDBSingleGroupBy1-8-1 timeseries"+strconv.Itoa(int(timeDelta)), func(b *testing.B) {
				b.ResetTimer()
				count := 0
				for iter := 0; iter < b.N; iter++ {
					for i := len(starts)/12*11; i < len(starts); i++ {
						q, err := db.Querier(starts[i], ends[i])
						testutil.Ok(b, err)

						for j := 0; j < 8; j++ {
							ss, err := q.Select(matchers[count%10], memstatsMatchers[4])
							testutil.Ok(b, err)

							for ss.Next() {
								it := ss.At().Iterator()
								for it.Next() {
									totalSamples += 1
								}
							}
							count++
						}
						q.Close()
					}
				}
			})

			// Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 1 hour.
			b.Run("BenchmarkGroupDBSingleGroupBy5-1-1 timeseries"+strconv.Itoa(int(timeDelta)), func(b *testing.B) {
				b.ResetTimer()
				for iter := 0; iter < b.N; iter++ {
					for i := len(starts)/12*11; i < len(starts); i++ {
						q, err := db.Querier(starts[i], ends[i])
						testutil.Ok(b, err)

						for j := 4; j < 9; j++ {
							ss, err := q.Select(matchers[iter%10], memstatsMatchers[j])
							testutil.Ok(b, err)

							for ss.Next() {
								it := ss.At().Iterator()
								for it.Next() {
									totalSamples += 1
								}
							}
						}
						q.Close()
					}
				}
			})

			// Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 12 hour.
			b.Run("BenchmarkGroupDBSingleGroupBy5-1-12 timeseries"+strconv.Itoa(int(timeDelta)), func(b *testing.B) {
				b.ResetTimer()
				for iter := 0; iter < b.N; iter++ {
					for i := 0; i < len(starts); i++ {
						q, err := db.Querier(starts[i], ends[i])
						testutil.Ok(b, err)

						for j := 4; j < 9; j++ {
							ss, err := q.Select(matchers[iter%10], memstatsMatchers[j])
							testutil.Ok(b, err)

							for ss.Next() {
								it := ss.At().Iterator()
								for it.Next() {
									totalSamples += 1
								}
							}
						}
						q.Close()
					}
				}
			})

			// Simple aggregrate (MAX) on 5 metrics for 8 hosts, every 5 mins for 1 hour.
			b.Run("BenchmarkGroupDBSingleGroupBy5-8-1 timeseries"+strconv.Itoa(int(timeDelta)), func(b *testing.B) {
				b.ResetTimer()
				count := 0
				for iter := 0; iter < b.N; iter++ {
					for i := len(starts)/12*11; i < len(starts); i++ {
						q, err := db.Querier(starts[i], ends[i])
						testutil.Ok(b, err)

						for k := 0; k < 8; k++ {
							for j := 4; j < 9; j++ {
								ss, err := q.Select(matchers[count%10], memstatsMatchers[j])
								testutil.Ok(b, err)

								for ss.Next() {
									it := ss.At().Iterator()
									for it.Next() {
										totalSamples += 1
									}
								}
							}
							count++
						}
						q.Close()
					}
				}
			})

			db.Close()
		}
	}
}
