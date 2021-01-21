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

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	tsdb "github.com/naivewong/tsdb-group"
	origin_tsdb "github.com/prometheus/tsdb"
	"github.com/naivewong/tsdb-group/chunkenc"
	"github.com/naivewong/tsdb-group/labels"
	origin_labels "github.com/prometheus/tsdb/labels"
	"gopkg.in/alecthomas/kingpin.v2"
)

var timeDelta = int64(15000)
var numNEData int

func init() {
	files, err := ioutil.ReadDir("../../testdata/bigdata/node_exporter/")
	if err != nil {
		fmt.Println("Error list folder ../../testdata/bigdata/node_exporter/")
		os.Exit(1)
	}

	for _, f := range files {
		if f.Name()[:4] == "data" {
			temp, _ := strconv.Atoi(f.Name()[4:])
			if temp > numNEData {
				numNEData = temp
			}
		}
	}
	numNEData++
}

func main() {
	var (
		defaultDBPath = filepath.Join("benchout", "storage")

		cli                    = kingpin.New(filepath.Base(os.Args[0]), "CLI tool for tsdb")
		headCmd                = cli.Command("head", "compare head size")
		benchCmd               = cli.Command("bench", "run benchmarks")
		benchWriteCmd          = benchCmd.Command("write", "run a write performance benchmark")
		benchWriteOutPath      = benchWriteCmd.Flag("out", "set the output path").Default("benchout").String()
		benchWriteNumMetrics   = benchWriteCmd.Flag("metrics", "number of metrics to read").Default("10000").Int()
		benchWriteSingleThread = benchWriteCmd.Flag("st", "use single thread to append data").Default("false").Bool()
		benchWriteTimeseries   = benchWriteCmd.Flag("timeseries", "use timeseries data").Default("false").Bool()
		benchWriteCounter      = benchWriteCmd.Flag("counter", "benchmark counter").Default("false").Bool()
		benchWriteTimeDelta    = benchWriteCmd.Flag("timedelta", "time interval between two samples").Default("15000").Int()
		benchWriteBatch        = benchWriteCmd.Flag("batch", "sharding size").Default("1000").Int()
		benchSamplesFile       = benchWriteCmd.Arg("file", "input file with samples data, default is ("+filepath.Join("..", "..", "testdata", "20kseries.json")+")").Default(filepath.Join("..", "..", "testdata", "20kseries.json")).String()
		benchNodeExporterFile  = benchWriteCmd.Arg("nefile", "node exporter labels file").Default(filepath.Join("..", "..", "testdata", "bigdata", "node_exporter", "timeseries.json")).String()
		benchScrapeCount       = benchWriteCmd.Flag("scrape", "number of samples per series").Default("2880").Int()
		benchGroupSize         = benchWriteCmd.Flag("gsize", "number of series per group").Default("500").Int()
		benchGroupSleep        = benchWriteCmd.Flag("gsleep", "sleep seconds").Default("180").Int()
		benchOriginSleep       = benchWriteCmd.Flag("osleep", "sleep seconds").Default("90").Int()
		benchNodeExporter      = benchWriteCmd.Flag("ne", "whether to use node exporter labels").Default("true").Bool()
		benchPrepareRandom     = benchWriteCmd.Flag("prandom", "whether to prepare random data in advance").Default("true").Bool()
		listCmd                = cli.Command("ls", "list db blocks")
		listCmdHumanReadable   = listCmd.Flag("human-readable", "print human readable values").Short('h').Bool()
		listPath               = listCmd.Arg("db path", "database path (default is "+defaultDBPath+")").Default(defaultDBPath).String()
		analyzeCmd             = cli.Command("analyze", "analyze churn, label pair cardinality.")
		analyzePath            = analyzeCmd.Arg("db path", "database path (default is "+defaultDBPath+")").Default(defaultDBPath).String()
		analyzeBlockID         = analyzeCmd.Arg("block id", "block to analyze (default is the last block)").String()
		analyzeLimit           = analyzeCmd.Flag("limit", "how many items to show in each list").Default("20").Int()
		dumpCmd                = cli.Command("dump", "dump samples from a TSDB")
		dumpPath               = dumpCmd.Arg("db path", "database path (default is "+defaultDBPath+")").Default(defaultDBPath).String()
		dumpMinTime            = dumpCmd.Flag("min-time", "minimum timestamp to dump").Default(strconv.FormatInt(math.MinInt64, 10)).Int64()
		dumpMaxTime            = dumpCmd.Flag("max-time", "maximum timestamp to dump").Default(strconv.FormatInt(math.MaxInt64, 10)).Int64()
	)

	safeDBOptions := *tsdb.DefaultOptions
	safeDBOptions.RetentionDuration = 0

	switch kingpin.MustParse(cli.Parse(os.Args[1:])) {
	case headCmd.FullCommand():
		compareHead()
	case benchWriteCmd.FullCommand():
		fmt.Println("--------------------------------------")
		fmt.Println("numMetrics:", *benchWriteNumMetrics)
		fmt.Println("samplesFile:", *benchSamplesFile)
		fmt.Println("scrapeCount:", *benchScrapeCount)
		fmt.Println("groupSize:", *benchGroupSize)
		fmt.Println("origin sleep:", *benchOriginSleep)
		fmt.Println("group sleep:", *benchGroupSleep)
		fmt.Println("batch:", *benchWriteBatch)
		fmt.Println("st:", *benchWriteSingleThread)
		fmt.Println("timeseries:", *benchWriteTimeseries)
		fmt.Println("counter:", *benchWriteCounter)
		fmt.Println("ne:", *benchNodeExporter)
		fmt.Println("nodeExporterFile:", *benchNodeExporterFile)
		fmt.Println("prepare random data:", *benchPrepareRandom)
		fmt.Println("--------------------------------------")
		timeDelta = int64(*benchWriteTimeDelta)
		var samples [][]float64
		if *benchPrepareRandom {
			samples = prepareRandomSamples(*benchScrapeCount, *benchWriteNumMetrics)
			fmt.Printf("random samples shape [%d, %d]\n", len(samples), len(samples[len(samples) - 1]))
		}
		owb := &originWriteBenchmark{
			outPath:          filepath.Join(*benchWriteOutPath, "origin"),
			numMetrics:       *benchWriteNumMetrics,
			samplesFile:      *benchSamplesFile,
			scrapeCount:      *benchScrapeCount,
			groupSize:        *benchGroupSize,
			sleep:            *benchOriginSleep,
			batch:            *benchWriteBatch,
			st:               *benchWriteSingleThread,
			timeseries:       *benchWriteTimeseries,
			counter:          *benchWriteCounter,
			ne:               *benchNodeExporter,
			nodeExporterFile: *benchNodeExporterFile,
			prandom:          *benchPrepareRandom,
			samples:          samples,
		}
		owb.run()
		fmt.Println(benchGroupSize, benchGroupSleep)
		// time.Sleep(40*time.Second)

		wb := &writeBenchmark{
			outPath:          filepath.Join(*benchWriteOutPath, "group"),
			numMetrics:       *benchWriteNumMetrics,
			samplesFile:      *benchSamplesFile,
			scrapeCount:      *benchScrapeCount,
			groupSize:        *benchGroupSize,
			sleep:            *benchGroupSleep,
			batch:            *benchWriteBatch,
			st:               *benchWriteSingleThread,
			timeseries:       *benchWriteTimeseries,
			counter:          *benchWriteCounter,
			ne:               *benchNodeExporter,
			nodeExporterFile: *benchNodeExporterFile,
			prandom:          *benchPrepareRandom,
			samples:          samples,
		}
		wb.run()
	case listCmd.FullCommand():
		db, err := tsdb.Open(*listPath, nil, nil, &safeDBOptions)
		if err != nil {
			exitWithError(err)
		}
		printBlocks(db.Blocks(), listCmdHumanReadable)
	case analyzeCmd.FullCommand():
		db, err := tsdb.Open(*analyzePath, nil, nil, &safeDBOptions)
		if err != nil {
			exitWithError(err)
		}
		blocks := db.Blocks()
		var block *tsdb.Block
		if *analyzeBlockID != "" {
			for _, b := range blocks {
				if b.Meta().ULID.String() == *analyzeBlockID {
					block = b
					break
				}
			}
		} else if len(blocks) > 0 {
			block = blocks[len(blocks)-1]
		}
		if block == nil {
			exitWithError(fmt.Errorf("block not found"))
		}
		analyzeBlock(block, *analyzeLimit)
	case dumpCmd.FullCommand():
		db, err := tsdb.Open(*dumpPath, nil, nil, &safeDBOptions)
		if err != nil {
			exitWithError(err)
		}
		dumpSamples(db, *dumpMinTime, *dumpMaxTime)
	}
}

func compareHead() {
	l := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	l = log.With(l, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)
	nums := []int{10100}
	for _, numSeries := range nums {
		if err := os.RemoveAll("head"); err != nil {
			exitWithError(err)
		}
		if err := os.MkdirAll("head", 0777); err != nil {
			exitWithError(err)
		}
		samples := prepareCounterSamples(7200, numSeries)
		{
			fmt.Println("----------- origin db random with numSeries", numSeries)
			var before runtime.MemStats
			var after runtime.MemStats
			db, err := origin_tsdb.Open("head/origin_random", l, nil, &origin_tsdb.Options{
				WALSegmentSize:    -1,
				RetentionDuration: 15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds
				BlockRanges:       origin_tsdb.ExponentialBlockRanges(2*60*60*1000, 5, 3),
			})

			f, err := os.Open("../../testdata/devops_series.json")
			if err != nil {
				exitWithError(err)
			}
			defer f.Close()

			var lsets []origin_labels.Labels
			olabels, err := readPrometheusLabels(f, numSeries)
			if err != nil {
				exitWithError(err)
			}
			for i := range olabels {
				var lset origin_labels.Labels
				for _, l := range olabels[i] {
					lset = append(lset, origin_labels.Label{Name: l.Name, Value: l.Value})
				}
				lsets = append(lsets, lset)
			}

			refs := make([]uint64, len(lsets))
			for i := 0; i < len(refs); i++ {
				refs[i] = 0
			}

			runtime.ReadMemStats(&before)
			fmt.Println("Sys:",before.Sys, "HeapAlloc:", before.HeapAlloc, "HeapSys:", before.HeapSys, "HeapIdle:", before.HeapIdle, "HeapInuse:", before.HeapInuse)
			for j := 0; j < 7199; j++ {
				t_ := int64(j*1000) + int64(rand.Float64()*float64(100))
				app := db.Appender()
				for i := 0; i < len(lsets); i++ {
					if refs[i] == 0 {
						ref, err := app.Add(lsets[i], t_, rand.Float64())
						if err != nil {
							exitWithError(err)
						}
						refs[i] = ref
					} else {
						err := app.AddFast(refs[i], t_, rand.Float64())
						if err != nil {
							exitWithError(err)
						}
					}
				}
				err := app.Commit()
				if err != nil {
					exitWithError(err)
				}
			}
			runtime.ReadMemStats(&after)
			fmt.Println("Sys:",after.Sys, "HeapAlloc:", after.HeapAlloc, "HeapSys:", after.HeapSys, "HeapIdle:", after.HeapIdle, "HeapInuse:", after.HeapInuse)
			fmt.Println("Sys:",after.Sys-before.Sys, "HeapAlloc:", after.HeapAlloc-before.HeapAlloc, "HeapSys:", after.HeapSys-before.HeapSys, "HeapIdle:", after.HeapIdle-before.HeapIdle, "HeapInuse:", after.HeapInuse-before.HeapInuse)
			// fmt.Println("Data size:", db.Head().Size())
			db.Close()
		}
		{
			fmt.Println("----------- group db random with numSeries", numSeries)
			var before runtime.MemStats
			var after runtime.MemStats
			db, err := tsdb.Open("head/group_random", l, nil, &tsdb.Options{
				WALSegmentSize:    -1,
				RetentionDuration: 15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds
				BlockRanges:       tsdb.ExponentialBlockRanges(2*60*60*1000, 5, 3),
			})
			f, err := os.Open("../../testdata/devops_series.json")
			if err != nil {
				exitWithError(err)
			}
			defer f.Close()

			lsets, err := readPrometheusLabels(f, numSeries)
			if err != nil {
				exitWithError(err)
			}

			refs := make([]uint64, (len(lsets)+101-1)/101)
			for i := 0; i < len(refs); i++ {
				refs[i] = 0
			}

			runtime.ReadMemStats(&before)
			fmt.Println("Sys:",before.Sys, "HeapAlloc:", before.HeapAlloc, "HeapSys:", before.HeapSys, "HeapIdle:", before.HeapIdle, "HeapInuse:", before.HeapInuse)
			for j := 0; j < 7199; j++ {
				t_ := int64(j*1000) + int64(rand.Float64()*float64(100))
				app := db.Appender()
				for i := 0; i < len(lsets); i += 101 {
					tempVals := make([]float64, 101)
					for k := 0; k < len(tempVals); k++ {
						tempVals[k] = rand.Float64()
					}
					if refs[i/101] == 0 {
						ref, err := app.AddGroup(lsets[i:i+101], t_, tempVals)
						if err != nil {
							exitWithError(err)
						}
						refs[i/101] = ref
					} else {
						err := app.AddGroupFast(refs[i/101], t_, tempVals)
						if err != nil {
							exitWithError(err)
						}
					}
				}
				err := app.Commit()
				if err != nil {
					exitWithError(err)
				}
			}
			runtime.ReadMemStats(&after)
			fmt.Println("Sys:",after.Sys, "HeapAlloc:", after.HeapAlloc, "HeapSys:", after.HeapSys, "HeapIdle:", after.HeapIdle, "HeapInuse:", after.HeapInuse)
			fmt.Println("Sys:",after.Sys-before.Sys, "HeapAlloc:", after.HeapAlloc-before.HeapAlloc, "HeapSys:", after.HeapSys-before.HeapSys, "HeapIdle:", after.HeapIdle-before.HeapIdle, "HeapInuse:", after.HeapInuse-before.HeapInuse)
			fmt.Println("Data size:", db.Head().Size())
			fmt.Println("NumSamples:", db.Head().NumSamples())
			db.Close()
		}
		{
			fmt.Println("----------- origin db timeseries with numSeries", numSeries)
			var before runtime.MemStats
			var after runtime.MemStats
			db, err := origin_tsdb.Open("head/origin_timeseries", l, nil, &origin_tsdb.Options{
				WALSegmentSize:    -1,
				RetentionDuration: 15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds
				BlockRanges:       origin_tsdb.ExponentialBlockRanges(2*60*60*1000, 5, 3),
			})

			f, err := os.Open("../../testdata/devops_series.json")
			if err != nil {
				exitWithError(err)
			}
			defer f.Close()

			var lsets []origin_labels.Labels
			olabels, err := readPrometheusLabels(f, numSeries)
			if err != nil {
				exitWithError(err)
			}
			for i := range olabels {
				var lset origin_labels.Labels
				for _, l := range olabels[i] {
					lset = append(lset, origin_labels.Label{Name: l.Name, Value: l.Value})
				}
				lsets = append(lsets, lset)
			}

			refs := make([]uint64, len(lsets))
			for i := 0; i < len(refs); i++ {
				refs[i] = 0
			}

			runtime.ReadMemStats(&before)
			fmt.Println("Sys:",before.Sys, "HeapAlloc:", before.HeapAlloc, "HeapSys:", before.HeapSys, "HeapIdle:", before.HeapIdle, "HeapInuse:", before.HeapInuse)
			for j := 0; j < 7199; j++ {
				t_ := int64(j*1000) + int64(rand.Float64()*float64(100))
				app := db.Appender()
				for i := 0; i < len(lsets); i++ {
					if refs[i] == 0 {
						ref, err := app.Add(lsets[i], t_, samples[j][i])
						if err != nil {
							exitWithError(err)
						}
						refs[i] = ref
					} else {
						err := app.AddFast(refs[i], t_, samples[j][i])
						if err != nil {
							exitWithError(err)
						}
					}
				}
				err := app.Commit()
				if err != nil {
					exitWithError(err)
				}
			}
			runtime.ReadMemStats(&after)
			fmt.Println("Sys:",after.Sys, "HeapAlloc:", after.HeapAlloc, "HeapSys:", after.HeapSys, "HeapIdle:", after.HeapIdle, "HeapInuse:", after.HeapInuse)
			fmt.Println("Sys:",after.Sys-before.Sys, "HeapAlloc:", after.HeapAlloc-before.HeapAlloc, "HeapSys:", after.HeapSys-before.HeapSys, "HeapIdle:", after.HeapIdle-before.HeapIdle, "HeapInuse:", after.HeapInuse-before.HeapInuse)
			// fmt.Println("Data size:", db.Head().Size())
			db.Close()
		}
		{
			fmt.Println("----------- group db timeseries with numSeries", numSeries)
			var before runtime.MemStats
			var after runtime.MemStats
			db, err := tsdb.Open("head/group_timeseries", l, nil, &tsdb.Options{
				WALSegmentSize:    -1,
				RetentionDuration: 15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds
				BlockRanges:       tsdb.ExponentialBlockRanges(2*60*60*1000, 5, 3),
			})
			f, err := os.Open("../../testdata/devops_series.json")
			if err != nil {
				exitWithError(err)
			}
			defer f.Close()

			lsets, err := readPrometheusLabels(f, numSeries)
			if err != nil {
				exitWithError(err)
			}

			refs := make([]uint64, (len(lsets)+101-1)/101)
			for i := 0; i < len(refs); i++ {
				refs[i] = 0
			}

			runtime.ReadMemStats(&before)
			fmt.Println("Sys:",before.Sys, "HeapAlloc:", before.HeapAlloc, "HeapSys:", before.HeapSys, "HeapIdle:", before.HeapIdle, "HeapInuse:", before.HeapInuse)
			for j := 0; j < 7199; j++ {
				t_ := int64(j*1000) + int64(rand.Float64()*float64(100))
				app := db.Appender()
				for i := 0; i < len(lsets); i += 101 {
					if refs[i/101] == 0 {
						ref, err := app.AddGroup(lsets[i:i+101], t_, samples[j][i:i+101])
						if err != nil {
							exitWithError(err)
						}
						refs[i/101] = ref
					} else {
						err := app.AddGroupFast(refs[i/101], t_, samples[j][i:i+101])
						if err != nil {
							exitWithError(err)
						}
					}
				}
				err := app.Commit()
				if err != nil {
					exitWithError(err)
				}
			}
			runtime.ReadMemStats(&after)
			fmt.Println("Sys:",after.Sys, "HeapAlloc:", after.HeapAlloc, "HeapSys:", after.HeapSys, "HeapIdle:", after.HeapIdle, "HeapInuse:", after.HeapInuse)
			fmt.Println("Sys:",after.Sys-before.Sys, "HeapAlloc:", after.HeapAlloc-before.HeapAlloc, "HeapSys:", after.HeapSys-before.HeapSys, "HeapIdle:", after.HeapIdle-before.HeapIdle, "HeapInuse:", after.HeapInuse-before.HeapInuse)
			fmt.Println("Data size:", db.Head().Size())
			fmt.Println("NumSamples:", db.Head().NumSamples())
			db.Close()
		}
	}
}

type writeBenchmark struct {
	outPath     string
	samplesFile string
	cleanup     bool
	numMetrics  int
	groupSize   int
	scrapeCount int
	sleep       int
	batch       int
	st          bool
	timeseries  bool
	counter     bool
	
	ne               bool
	nodeExporterFile string
	prandom          bool
	samples         [][]float64

	storage *tsdb.DB

	cpuprof   *os.File
	memprof   *os.File
	blockprof *os.File
	mtxprof   *os.File
}

func (b *writeBenchmark) run() {
	if b.outPath == "" {
		dir, err := ioutil.TempDir("", "tsdb_bench")
		if err != nil {
			exitWithError(err)
		}
		b.outPath = dir
		b.cleanup = true
	}
	if err := os.RemoveAll(b.outPath); err != nil {
		exitWithError(err)
	}
	if err := os.MkdirAll(b.outPath, 0777); err != nil {
		exitWithError(err)
	}

	dir := filepath.Join(b.outPath, "storage")

	l := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	l = log.With(l, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)

	st, err := tsdb.Open(dir, l, nil, &tsdb.Options{
		RetentionDuration: 15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds
		BlockRanges:       tsdb.ExponentialBlockRanges(2*60*60*1000, 5, 3),
	})
	if err != nil {
		exitWithError(err)
	}
	b.storage = st

	var lsets []labels.Labels

	if b.timeseries {
		b.samples = prepareNodeExporterSamples(b.numMetrics, b.groupSize)
		fmt.Println("node exporter samples shape", len(b.samples), len(b.samples[len(b.samples) - 1]))
	}

	measureTime("readData", func() {
		f, err := os.Open(b.samplesFile)
		if err != nil {
			exitWithError(err)
		}
		defer f.Close()

		if b.ne {
			lsets, err = readNodeExporterLabels(b.nodeExporterFile, b.numMetrics, b.groupSize)	
		} else {
			lsets, err = readPrometheusLabels(f, b.numMetrics)
		}
		if err != nil {
			exitWithError(err)
		}
	})

	// g := &labels.ByGroup{Lsets: &lsets, GSize: b.groupSize}
	// sort.Sort(g)

	var total uint64

	dur := measureTime("ingestScrapes", func() {
		// b.startProfiling()
		loc, _ := time.LoadLocation("Local")
		fmt.Println("After startProfiling", time.Now().In(loc))
		if b.st {
			total, err = b.ingestScrapesSingleThread(lsets, b.scrapeCount)
		} else {
			if b.timeseries || b.prandom {
				total, err = b.ingestScrapesNodeExporter(lsets, b.scrapeCount, b.samples)
			} else if b.counter {
				total, err = b.ingestScrapesCounter(lsets, b.scrapeCount)
			} else {
				total, err = b.ingestScrapes(lsets, b.scrapeCount)
			}
		}
		if err != nil {
			exitWithError(err)
		}
		fmt.Println("After ingest", time.Now().In(loc))
	})

	fmt.Println(" > total samples:", total)
	fmt.Println(" > samples/sec:", float64(total)/dur.Seconds())

	time.Sleep(time.Duration(b.sleep)*time.Second)

	measureTime("stopStorage", func() {
		// if err := b.stopProfiling(); err != nil {
		// 	exitWithError(err)
		// }
		if err := b.storage.Close(); err != nil {
			exitWithError(err)
		}
		// if err := b.stopProfiling(); err != nil {
		// 	exitWithError(err)
		// }
	})
}

func prepareCounterSamples(row, column int) [][]float64 {
	data := make([][]float64, row)
	start := 123456789.0
	for i := range data {
		data[i] = make([]float64, column)
		for j := range data[i] {
			data[i][j] = start
		}
		start += float64(1000)
	}
	return data
}

func prepareRandomSamples(row, column int) [][]float64 {
	data := make([][]float64, row)
	for i := range data {
		data[i] = make([]float64, column)
		for j := range data[i] {
			data[i][j] = rand.Float64()
		}
	}
	return data
}

func prepareNodeExporterSamples(column, gsize int) [][]float64 {
	scanners := make([]*bufio.Scanner, 0, (column+gsize-1)/gsize)
	files := make([]*os.File, 0, (column+gsize-1)/gsize)
	length := (column+gsize-1)/gsize
	if length > numNEData {
		length = numNEData
	}
	for j := 0; j < length; j++ {
		file, _ := os.Open("../../testdata/bigdata/node_exporter/data" + strconv.Itoa(j))
		scanners = append(scanners, bufio.NewScanner(file))
		files = append(files, file)
	}
	length = (column+gsize-1)/gsize - numNEData
	for length > 0 {
		tempLen := length
		if length > numNEData {
			tempLen = numNEData
		}
		for j := 0; j < tempLen; j++ {
			file, _ := os.Open("../../testdata/bigdata/node_exporter/data" + strconv.Itoa(j))
			scanners = append(scanners, bufio.NewScanner(file))
			files = append(files, file)
		}
		length -= tempLen
	}

	data := [][]float64{}
	for scanners[0].Scan() {
		tempData := make([]float64, 0, column)
		{
			s := scanners[0].Text()
			data := strings.Split(s, ",")
			for j := 0; j < gsize; j++ {
				v, _ := strconv.ParseFloat(data[j], 64)
				tempData = append(tempData, v)
			}

			for i := 0; i < int(timeDelta)/5000-1; i++ {
				scanners[0].Scan()
			}
		}
		for sc := 1; sc < len(scanners); sc++ {
			scanners[sc].Scan()
			s := scanners[sc].Text()
			data := strings.Split(s, ",")
			for j := 0; j < gsize; j++ {
				v, _ := strconv.ParseFloat(data[j], 64)
				tempData = append(tempData, v)
			}

			for i := 0; i < int(timeDelta)/5000-1; i++ {
				scanners[sc].Scan()
			}
		}
		data = append(data, tempData)
	}
	for _, f := range files {
		f.Close()
	}
	return data
}

func (b *writeBenchmark) ingestScrapes(lbls []labels.Labels, scrapeCount int) (uint64, error) {
	var total uint64

	generators := make([]*rand.Rand, len(lbls)/b.batch)
	for i := range generators {
		generators[i] = rand.New(rand.NewSource(int64(i)))
	}

	for i := 0; i < scrapeCount; i += 100 {
		var wg sync.WaitGroup
		lbls := lbls
		gid := 0
		for len(lbls) > 0 {
			l := b.batch
			if len(lbls) < b.batch {
				l = len(lbls)
			}
			batch := lbls[:l]
			lbls = lbls[l:]

			wg.Add(1)
			tempGenerator := generators[gid]
			length := 100
			if i + length > scrapeCount {
				length = scrapeCount - i
			}
			go func() {
				n, err := b.ingestScrapesShard(batch, length, int64(i)*timeDelta, tempGenerator)
				if err != nil {
					// exitWithError(err)
					fmt.Println(" err", err)
				}
				atomic.AddUint64(&total, n)
				wg.Done()
			}()
			gid++
		}
		wg.Wait()
	}
	fmt.Println("ingestion completed")

	return total, nil
}

func (b *writeBenchmark) ingestScrapesCounter(lbls []labels.Labels, scrapeCount int) (uint64, error) {
	var total uint64

	start := time.Now()
	segTotal := float64(200*len(lbls))

	for i := 0; i < scrapeCount; i += 100 {
		var wg sync.WaitGroup
		lbls := lbls
		for len(lbls) > 0 {
			l := b.batch
			if len(lbls) < b.batch {
				l = len(lbls)
			}
			batch := lbls[:l]
			lbls = lbls[l:]

			wg.Add(1)
			go func() {
				n, err := b.ingestScrapesShardCounter(batch, 100, int64(i)*timeDelta)
				if err != nil {
					// exitWithError(err)
					fmt.Println(" err", err)
				}
				atomic.AddUint64(&total, n)
				wg.Done()
			}()
		}
		wg.Wait()
		if i > 0 && i % 200 == 0 {
			cur := time.Now()
			fmt.Println("--- seg tp ", segTotal/cur.Sub(start).Seconds())
			start = cur
			// b.stopProfiling()
			// return total, nil
		}
	}
	fmt.Println("counter ingestion completed")

	return total, nil
}

func (b *writeBenchmark) ingestScrapesNodeExporter(lbls []labels.Labels, scrapeCount int, samples [][]float64) (uint64, error) {
	var total uint64

	for i := 0; i < scrapeCount; i += 100 {
		var wg sync.WaitGroup
		lbls := lbls
		for len(lbls) > 0 {
			l := b.batch
			if len(lbls) < b.batch {
				l = len(lbls)
			}
			batch := lbls[:l]
			lbls = lbls[l:]

			wg.Add(1)
			go func() {
				n, err := b.ingestScrapesShardNodeExporter(batch, 100, int64(i)*timeDelta, i, samples)
				if err != nil {
					// exitWithError(err)
					fmt.Println(" err", err)
				}
				atomic.AddUint64(&total, n)
				wg.Done()
			}()
		}
		wg.Wait()
	}
	fmt.Println("node exporter ingestion completed")

	return total, nil
}

func (b *writeBenchmark) ingestScrapesSingleThread(lbls []labels.Labels, scrapeCount int) (uint64, error) {
	type group struct {
		labels []labels.Labels
		values  []float64
		ref    *uint64
	}

	var total uint64
	var ts    int64

	scrape := make([]*group, 0, (len(lbls)+b.groupSize-1)/b.groupSize)
	for i := 0; i < len(lbls); i += b.groupSize {
		scrape = append(scrape, &group{
			labels: lbls[i:i+b.groupSize],
			values: make([]float64, b.groupSize),
		})
	}

	for i := 0; i < scrapeCount; i++ {
		app := b.storage.Appender()
		ts += timeDelta + int64(rand.Intn(100))

		for _, g := range scrape {
			for j := 0; j < b.groupSize; j++ {
				g.values[j] = rand.Float64()
			}

			if g.ref == nil {
				ref, err := app.AddGroup(g.labels, ts, g.values)
				if err != nil {
					// panic(err)
					continue
				}
				g.ref = &ref
			} else if err := app.AddGroupFast(*g.ref, ts, g.values); err != nil {

				if errors.Cause(err) != tsdb.ErrNotFound {
					panic(err)
				}

				ref, err := app.AddGroup(g.labels, ts, g.values)
				if err != nil {
					// panic(err)
					continue
				}
				g.ref = &ref
			}

			total+=uint64(b.groupSize)
		}
		if err := app.Commit(); err != nil {
			return total, err
		}
	}
	fmt.Println("single thread ingestion completed")

	return total, nil
}

func (b *writeBenchmark) ingestScrapesShard(lbls []labels.Labels, scrapeCount int, baset int64, generator *rand.Rand) (uint64, error) {
	ts := baset

	type group struct {
		labels []labels.Labels
		values  []float64
		ref    *uint64
	}

	scrape := make([]*group, 0, (len(lbls)+b.groupSize-1)/b.groupSize)

	for i := 0; i < len(lbls); i += b.groupSize {
		scrape = append(scrape, &group{
			labels: lbls[i:i+b.groupSize],
			values: make([]float64, b.groupSize),
		})
	}
	total := uint64(0)

	for i := 0; i < scrapeCount; i++ {
		app := b.storage.Appender()
		ts += timeDelta + int64(generator.Intn(100))

		for _, g := range scrape {
			for j := 0; j < b.groupSize; j++ {
				g.values[j] = generator.Float64()
			}

			if g.ref == nil {
				ref, err := app.AddGroup(g.labels, ts, g.values)
				if err != nil {
					// panic(err)
					continue
				}
				g.ref = &ref
			} else if err := app.AddGroupFast(*g.ref, ts, g.values); err != nil {

				if errors.Cause(err) != tsdb.ErrNotFound {
					panic(err)
				}

				ref, err := app.AddGroup(g.labels, ts, g.values)
				if err != nil {
					// panic(err)
					continue
				}
				g.ref = &ref
			}

			total+=uint64(b.groupSize)
		}
		if err := app.Commit(); err != nil {
			return total, err
		}
	}
	return total, nil
}

func (b *writeBenchmark) ingestScrapesShardCounter(lbls []labels.Labels, scrapeCount int, baset int64) (uint64, error) {
	ts := baset

	type group struct {
		labels []labels.Labels
		values  []float64
		ref    *uint64
	}

	scrape := make([]*group, 0, (len(lbls)+b.groupSize-1)/b.groupSize)

	for i := 0; i < len(lbls); i += b.groupSize {
		values := make([]float64, b.groupSize)
		for j := range values {
			values[j] = float64(123456789)
		}
		scrape = append(scrape, &group{
			labels: lbls[i:i+b.groupSize],
			values: values,
		})
	}
	total := uint64(0)

	for i := 0; i < scrapeCount; i++ {
		app := b.storage.Appender()
		ts += timeDelta + int64(rand.Intn(100))

		for _, g := range scrape {
			for j := 0; j < b.groupSize; j++ {
				g.values[j] += float64(1000)
			}

			if g.ref == nil {
				ref, err := app.AddGroup(g.labels, ts, g.values)
				if err != nil {
					// panic(err)
					continue
				}
				g.ref = &ref
			} else if err := app.AddGroupFast(*g.ref, ts, g.values); err != nil {

				if errors.Cause(err) != tsdb.ErrNotFound {
					panic(err)
				}

				ref, err := app.AddGroup(g.labels, ts, g.values)
				if err != nil {
					// panic(err)
					continue
				}
				g.ref = &ref
			}

			total+=uint64(b.groupSize)
		}
		if err := app.Commit(); err != nil {
			return total, err
		}
	}
	return total, nil
}

func (b *writeBenchmark) ingestScrapesShardNodeExporter(lbls []labels.Labels, scrapeCount int, baset int64, idx int, samples [][]float64) (uint64, error) {
	ts := baset

	type group struct {
		labels []labels.Labels
		values  []float64
		ref    *uint64
	}

	scrape := make([]*group, 0, (len(lbls)+b.groupSize-1)/b.groupSize)

	for i := 0; i < len(lbls); i += b.groupSize {
		scrape = append(scrape, &group{
			labels: lbls[i:i+b.groupSize],
			values: make([]float64, b.groupSize),
		})
	}
	total := uint64(0)

	end := idx + scrapeCount
	if end > len(samples) {
		end = len(samples)
	}
	for i := idx; i < end; i++ {
		app := b.storage.Appender()
		ts += timeDelta + int64(rand.Intn(100))

		for gid, g := range scrape {
			for j := 0; j < b.groupSize; j++ {
				g.values[j] = samples[i][gid*b.groupSize+j]
			}

			if g.ref == nil {
				ref, err := app.AddGroup(g.labels, ts, g.values)
				if err != nil {
					// panic(err)
					continue
				}
				g.ref = &ref
			} else if err := app.AddGroupFast(*g.ref, ts, g.values); err != nil {

				if errors.Cause(err) != tsdb.ErrNotFound {
					panic(err)
				}

				ref, err := app.AddGroup(g.labels, ts, g.values)
				if err != nil {
					// panic(err)
					continue
				}
				g.ref = &ref
			}

			total+=uint64(b.groupSize)
		}
		if err := app.Commit(); err != nil {
			return total, err
		}
	}
	return total, nil
}

func (b *writeBenchmark) startProfiling() {
	var err error

	// Start CPU profiling.
	b.cpuprof, err = os.Create(filepath.Join(b.outPath, "cpu.prof"))
	if err != nil {
		exitWithError(fmt.Errorf("bench: could not create cpu profile: %v", err))
	}
	if err := pprof.StartCPUProfile(b.cpuprof); err != nil {
		exitWithError(fmt.Errorf("bench: could not start CPU profile: %v", err))
	}

	// Start memory profiling.
	b.memprof, err = os.Create(filepath.Join(b.outPath, "mem.prof"))
	if err != nil {
		exitWithError(fmt.Errorf("bench: could not create memory profile: %v", err))
	}
	runtime.MemProfileRate = 64 * 1024

	// Start fatal profiling.
	b.blockprof, err = os.Create(filepath.Join(b.outPath, "block.prof"))
	if err != nil {
		exitWithError(fmt.Errorf("bench: could not create block profile: %v", err))
	}
	runtime.SetBlockProfileRate(20)

	b.mtxprof, err = os.Create(filepath.Join(b.outPath, "mutex.prof"))
	if err != nil {
		exitWithError(fmt.Errorf("bench: could not create mutex profile: %v", err))
	}
	runtime.SetMutexProfileFraction(20)
}

func (b *writeBenchmark) stopProfiling() error {
	if b.cpuprof != nil {
		pprof.StopCPUProfile()
		b.cpuprof.Close()
		b.cpuprof = nil
	}
	if b.memprof != nil {
		if err := pprof.Lookup("heap").WriteTo(b.memprof, 0); err != nil {
			return fmt.Errorf("error writing mem profile: %v", err)
		}
		b.memprof.Close()
		b.memprof = nil
	}
	if b.blockprof != nil {
		if err := pprof.Lookup("block").WriteTo(b.blockprof, 0); err != nil {
			return fmt.Errorf("error writing block profile: %v", err)
		}
		b.blockprof.Close()
		b.blockprof = nil
		runtime.SetBlockProfileRate(0)
	}
	if b.mtxprof != nil {
		if err := pprof.Lookup("mutex").WriteTo(b.mtxprof, 0); err != nil {
			return fmt.Errorf("error writing mutex profile: %v", err)
		}
		b.mtxprof.Close()
		b.mtxprof = nil
		runtime.SetMutexProfileFraction(0)
	}
	return nil
}

type originWriteBenchmark struct {
	outPath     string
	samplesFile string
	cleanup     bool
	numMetrics  int
	scrapeCount int
	groupSize   int
	sleep       int
	batch       int
	st          bool
	timeseries  bool
	counter     bool

	ne               bool
	nodeExporterFile string
	prandom          bool
	samples          [][]float64

	storage *origin_tsdb.DB

	cpuprof   *os.File
	memprof   *os.File
	blockprof *os.File
	mtxprof   *os.File
}

func (b *originWriteBenchmark) run() {
	if b.outPath == "" {
		dir, err := ioutil.TempDir("", "tsdb_origin_bench")
		if err != nil {
			exitWithError(err)
		}
		b.outPath = dir
		b.cleanup = true
	}
	if err := os.RemoveAll(b.outPath); err != nil {
		exitWithError(err)
	}
	if err := os.MkdirAll(b.outPath, 0777); err != nil {
		exitWithError(err)
	}

	dir := filepath.Join(b.outPath, "storage")

	l := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	l = log.With(l, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)

	st, err := origin_tsdb.Open(dir, l, nil, &origin_tsdb.Options{
		RetentionDuration: 15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds
		BlockRanges:       origin_tsdb.ExponentialBlockRanges(2*60*60*1000, 5, 3),
	})
	if err != nil {
		exitWithError(err)
	}
	b.storage = st

	var olabels []labels.Labels
	var labels []origin_labels.Labels

	measureTime("readData", func() {
		f, err := os.Open(b.samplesFile)
		if err != nil {
			exitWithError(err)
		}
		defer f.Close()

		if b.ne {
			olabels, err = readNodeExporterLabels(b.nodeExporterFile, b.numMetrics, b.groupSize)	
		} else {
			olabels, err = readPrometheusLabels(f, b.numMetrics)
		}
		if err != nil {
			exitWithError(err)
		}
	})
	for i := range olabels {
		var lset origin_labels.Labels
		for _, l := range olabels[i] {
			lset = append(lset, origin_labels.Label{Name: l.Name, Value: l.Value})
		}
		labels = append(labels, lset)
	}

	var total uint64

	if b.timeseries {
		b.samples = prepareNodeExporterSamples(b.numMetrics, b.groupSize)
		fmt.Println("node exporter samples shape", len(b.samples), len(b.samples[len(b.samples) - 1]))
	}

	dur := measureTime("ingestScrapes", func() {
		// b.startProfiling()
		loc, _ := time.LoadLocation("Local")
		fmt.Println("After startProfiling", time.Now().In(loc))
		if b.st {
			total, err = b.ingestScrapesSingleThread(labels, b.scrapeCount)
		} else {
			if b.timeseries || b.prandom {
				total, err = b.ingestScrapesNodeExporter(labels, b.scrapeCount, b.samples)
			} else if b.counter {
				total, err = b.ingestScrapesCounter(labels, b.scrapeCount)
			} else {
				total, err = b.ingestScrapes(labels, b.scrapeCount)
			}
		}
		if err != nil {
			exitWithError(err)
		}
		fmt.Println("After ingest", time.Now().In(loc))
	})

	fmt.Println(" > total samples:", total)
	fmt.Println(" > samples/sec:", float64(total)/dur.Seconds())

	time.Sleep(time.Duration(b.sleep)*time.Second)

	measureTime("stopStorage", func() {
		// if err := b.stopProfiling(); err != nil {
		// 	exitWithError(err)
		// }
		if err := b.storage.Close(); err != nil {
			exitWithError(err)
		}
		// if err := b.stopProfiling(); err != nil {
		// 	exitWithError(err)
		// }
	})
}

func (b *originWriteBenchmark) ingestScrapes(lbls []origin_labels.Labels, scrapeCount int) (uint64, error) {
	var total uint64

	generators := make([]*rand.Rand, len(lbls)/b.batch)
	for i := range generators {
		generators[i] = rand.New(rand.NewSource(int64(i)))
	}

	for i := 0; i < scrapeCount; i += 100 {
		var wg sync.WaitGroup
		lbls := lbls
		gid := 0
		for len(lbls) > 0 {
			l := b.batch
			if len(lbls) < b.batch {
				l = len(lbls)
			}
			batch := lbls[:l]
			lbls = lbls[l:]

			wg.Add(1)
			tempGenerator := generators[gid]
			length := 100
			if i + length > scrapeCount {
				length = scrapeCount - i
			}
			go func() {
				n, err := b.ingestScrapesShard(batch, length, int64(i)*timeDelta, tempGenerator)
				if err != nil {
					// exitWithError(err)
					fmt.Println(" err", err)
				}
				atomic.AddUint64(&total, n)
				wg.Done()
			}()
			gid++
		}
		wg.Wait()
	}
	fmt.Println("ingestion completed")

	return total, nil
}

func (b *originWriteBenchmark) ingestScrapesCounter(lbls []origin_labels.Labels, scrapeCount int) (uint64, error) {
	var total uint64

	start := time.Now()
	segTotal := float64(200*len(lbls))

	for i := 0; i < scrapeCount; i += 100 {
		var wg sync.WaitGroup
		lbls := lbls
		for len(lbls) > 0 {
			l := b.batch
			if len(lbls) < b.batch {
				l = len(lbls)
			}
			batch := lbls[:l]
			lbls = lbls[l:]

			wg.Add(1)
			go func() {
				n, err := b.ingestScrapesShardCounter(batch, 100, int64(i)*timeDelta)
				if err != nil {
					// exitWithError(err)
					fmt.Println(" err", err)
				}
				atomic.AddUint64(&total, n)
				wg.Done()
			}()
		}
		wg.Wait()
		if i > 0 && i % 200 == 0 {
			cur := time.Now()
			fmt.Println("--- seg tp ", segTotal/cur.Sub(start).Seconds())
			start = cur
			// b.stopProfiling()
			// return total, nil
		}
	}
	fmt.Println("ingestion completed")

	return total, nil
}

func (b *originWriteBenchmark) ingestScrapesNodeExporter(lbls []origin_labels.Labels, scrapeCount int, samples [][]float64) (uint64, error) {
	var total uint64

	for i := 0; i < scrapeCount; i += 100 {
		var wg sync.WaitGroup
		lbls := lbls
		for len(lbls) > 0 {
			l := b.batch
			if len(lbls) < b.batch {
				l = len(lbls)
			}
			batch := lbls[:l]
			lbls = lbls[l:]

			wg.Add(1)
			go func() {
				n, err := b.ingestScrapesShardNodeExporter(batch, 100, int64(i)*timeDelta, i, samples)
				if err != nil {
					// exitWithError(err)
					fmt.Println(" err", err)
				}
				atomic.AddUint64(&total, n)
				wg.Done()
			}()
		}
		wg.Wait()
	}
	fmt.Println("ingestion completed")

	return total, nil
}

func (b *originWriteBenchmark) ingestScrapesSingleThread(lbls []origin_labels.Labels, scrapeCount int) (uint64, error) {
	type sample struct {
		labels origin_labels.Labels
		value  float64
		ref    *uint64
	}

	var total uint64
	var ts    int64

	scrape := make([]*sample, 0, len(lbls))
	for _, m := range lbls {
		scrape = append(scrape, &sample{
			labels: m,
		})
	}

	for i := 0; i < scrapeCount; i++ {
		app := b.storage.Appender()
		ts += timeDelta + int64(rand.Intn(100))

		for _, s := range scrape {
			s.value = rand.Float64()

			if s.ref == nil {
				ref, err := app.Add(s.labels, ts, s.value)
				if err != nil {
					// panic(err)
					continue
				}
				s.ref = &ref
			} else if err := app.AddFast(*s.ref, ts, s.value); err != nil {

				if errors.Cause(err) != origin_tsdb.ErrNotFound {
					panic(err)
				}

				ref, err := app.Add(s.labels, ts, s.value)
				if err != nil {
					// panic(err)
					continue
				}
				s.ref = &ref
			}

			total++
		}
		if err := app.Commit(); err != nil {
			return total, err
		}
	}
	fmt.Println("single thread ingestion completed")

	return total, nil
}

func (b *originWriteBenchmark) ingestScrapesShard(lbls []origin_labels.Labels, scrapeCount int, baset int64, generator *rand.Rand) (uint64, error) {
	ts := baset

	type sample struct {
		labels origin_labels.Labels
		value  float64
		ref    *uint64
	}

	scrape := make([]*sample, 0, len(lbls))

	for _, m := range lbls {
		scrape = append(scrape, &sample{
			labels: m,
		})
	}
	total := uint64(0)

	for i := 0; i < scrapeCount; i++ {
		app := b.storage.Appender()
		ts += timeDelta + int64(generator.Intn(100))

		for _, s := range scrape {
			s.value = generator.Float64()

			if s.ref == nil {
				ref, err := app.Add(s.labels, ts, s.value)
				if err != nil {
					// panic(err)
					continue
				}
				s.ref = &ref
			} else if err := app.AddFast(*s.ref, ts, s.value); err != nil {

				if errors.Cause(err) != origin_tsdb.ErrNotFound {
					panic(err)
				}

				ref, err := app.Add(s.labels, ts, s.value)
				if err != nil {
					// panic(err)
					continue
				}
				s.ref = &ref
			}

			total++
		}
		if err := app.Commit(); err != nil {
			return total, err
		}
	}
	return total, nil
}

func (b *originWriteBenchmark) ingestScrapesShardCounter(lbls []origin_labels.Labels, scrapeCount int, baset int64) (uint64, error) {
	ts := baset

	type sample struct {
		labels origin_labels.Labels
		value  float64
		ref    *uint64
	}

	scrape := make([]*sample, 0, len(lbls))

	for _, m := range lbls {
		scrape = append(scrape, &sample{
			labels: m,
			value: float64(123456789),
		})
	}
	total := uint64(0)

	for i := 0; i < scrapeCount; i++ {
		app := b.storage.Appender()
		ts += timeDelta + int64(rand.Intn(100))

		for _, s := range scrape {
			s.value += float64(1000)

			if s.ref == nil {
				ref, err := app.Add(s.labels, ts, s.value)
				if err != nil {
					// panic(err)
					continue
				}
				s.ref = &ref
			} else if err := app.AddFast(*s.ref, ts, s.value); err != nil {

				if errors.Cause(err) != origin_tsdb.ErrNotFound {
					panic(err)
				}

				ref, err := app.Add(s.labels, ts, s.value)
				if err != nil {
					// panic(err)
					continue
				}
				s.ref = &ref
			}

			total++
		}
		if err := app.Commit(); err != nil {
			return total, err
		}
	}
	return total, nil
}

func (b *originWriteBenchmark) ingestScrapesShardNodeExporter(lbls []origin_labels.Labels, scrapeCount int, baset int64, idx int, samples [][]float64) (uint64, error) {
	ts := baset

	type sample struct {
		labels origin_labels.Labels
		value  float64
		ref    *uint64
	}

	scrape := make([]*sample, 0, len(lbls))

	for _, m := range lbls {
		scrape = append(scrape, &sample{
			labels: m,
		})
	}
	total := uint64(0)

	end := idx + scrapeCount
	if end > len(samples) {
		end = len(samples)
	}
	for i := idx; i < end; i++ {
		app := b.storage.Appender()
		ts += timeDelta + int64(rand.Intn(100))

		for sid, s := range scrape {
			s.value = samples[i][sid]

			if s.ref == nil {
				ref, err := app.Add(s.labels, ts, s.value)
				if err != nil {
					// panic(err)
					continue
				}
				s.ref = &ref
			} else if err := app.AddFast(*s.ref, ts, s.value); err != nil {

				if errors.Cause(err) != origin_tsdb.ErrNotFound {
					panic(err)
				}

				ref, err := app.Add(s.labels, ts, s.value)
				if err != nil {
					// panic(err)
					continue
				}
				s.ref = &ref
			}

			total++
		}
		if err := app.Commit(); err != nil {
			return total, err
		}
	}
	return total, nil
}

func (b *originWriteBenchmark) startProfiling() {
	var err error

	// Start CPU profiling.
	b.cpuprof, err = os.Create(filepath.Join(b.outPath, "cpu.prof"))
	if err != nil {
		exitWithError(fmt.Errorf("bench: could not create cpu profile: %v", err))
	}
	if err := pprof.StartCPUProfile(b.cpuprof); err != nil {
		exitWithError(fmt.Errorf("bench: could not start CPU profile: %v", err))
	}

	// Start memory profiling.
	b.memprof, err = os.Create(filepath.Join(b.outPath, "mem.prof"))
	if err != nil {
		exitWithError(fmt.Errorf("bench: could not create memory profile: %v", err))
	}
	runtime.MemProfileRate = 64 * 1024

	// Start fatal profiling.
	b.blockprof, err = os.Create(filepath.Join(b.outPath, "block.prof"))
	if err != nil {
		exitWithError(fmt.Errorf("bench: could not create block profile: %v", err))
	}
	runtime.SetBlockProfileRate(20)

	b.mtxprof, err = os.Create(filepath.Join(b.outPath, "mutex.prof"))
	if err != nil {
		exitWithError(fmt.Errorf("bench: could not create mutex profile: %v", err))
	}
	runtime.SetMutexProfileFraction(20)
}

func (b *originWriteBenchmark) stopProfiling() error {
	if b.cpuprof != nil {
		pprof.StopCPUProfile()
		b.cpuprof.Close()
		b.cpuprof = nil
	}
	if b.memprof != nil {
		if err := pprof.Lookup("heap").WriteTo(b.memprof, 0); err != nil {
			return fmt.Errorf("error writing mem profile: %v", err)
		}
		b.memprof.Close()
		b.memprof = nil
	}
	if b.blockprof != nil {
		if err := pprof.Lookup("block").WriteTo(b.blockprof, 0); err != nil {
			return fmt.Errorf("error writing block profile: %v", err)
		}
		b.blockprof.Close()
		b.blockprof = nil
		runtime.SetBlockProfileRate(0)
	}
	if b.mtxprof != nil {
		if err := pprof.Lookup("mutex").WriteTo(b.mtxprof, 0); err != nil {
			return fmt.Errorf("error writing mutex profile: %v", err)
		}
		b.mtxprof.Close()
		b.mtxprof = nil
		runtime.SetMutexProfileFraction(0)
	}
	return nil
}

func measureTime(stage string, f func()) time.Duration {
	fmt.Printf(">> start stage=%s\n", stage)
	start := time.Now()
	f()
	fmt.Printf(">> completed stage=%s duration=%s\n", stage, time.Since(start))
	return time.Since(start)
}

func readNodeExporterLabels(filename string, n, gsize int) ([]labels.Labels, error) {
	var mets []labels.Labels
	for j := 0; j < n / gsize; j++ {
		file, err := os.Open(filename)
		if err != nil {
			return nil, err
		}

		scanner := bufio.NewScanner(file)
		count := 0
		for scanner.Scan() && count < gsize {
			m := make(map[string]string)
			err = json.Unmarshal([]byte(scanner.Text()), &m)
			m["instance"] = fmt.Sprintf("pc9%06d:9100", j)
			lset := labels.FromMap(m)
			sort.Sort(lset)
			mets = append(mets, lset)
			count++
		}
		file.Close()
	}
	return mets, nil
}

func readPrometheusLabels(r io.Reader, n int) ([]labels.Labels, error) {
	scanner := bufio.NewScanner(r)

	var mets []labels.Labels
	hashes := map[uint64]struct{}{}
	i := 0
	for scanner.Scan() && i < n {
		m := make(labels.Labels, 0, 10)

		r := strings.NewReplacer("\"", "", "{", "", "}", "")
		s := r.Replace(scanner.Text())

		labelChunks := strings.Split(s, ",")
		for _, labelChunk := range labelChunks {
			split := strings.Split(labelChunk, ":")
			m = append(m, labels.Label{Name: split[0], Value: split[1]})
		}
		// Order of the k/v labels matters, don't assume we'll always receive them already sorted.
		sort.Sort(m)
		h := m.Hash()
		if _, ok := hashes[h]; ok {
			continue
		}
		mets = append(mets, m)
		hashes[h] = struct{}{}
		i++
	}
	// sort.Slice(mets, func(i, j int) bool { return labels.Compare(mets[i], mets[j]) < 0 })
	return mets, nil
}

func exitWithError(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func printBlocks(blocks []*tsdb.Block, humanReadable *bool) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer tw.Flush()

	fmt.Fprintln(tw, "BLOCK ULID\tMIN TIME\tMAX TIME\tNUM SAMPLES\tNUM CHUNKS\tNUM SERIES")
	for _, b := range blocks {
		meta := b.Meta()

		fmt.Fprintf(tw,
			"%v\t%v\t%v\t%v\t%v\t%v\n",
			meta.ULID,
			getFormatedTime(meta.MinTime, humanReadable),
			getFormatedTime(meta.MaxTime, humanReadable),
			meta.Stats.NumSamples,
			meta.Stats.NumChunks,
			meta.Stats.NumSeries,
		)
	}
}

func getFormatedTime(timestamp int64, humanReadable *bool) string {
	if *humanReadable {
		return time.Unix(timestamp/1000, 0).String()
	}
	return strconv.FormatInt(timestamp, 10)
}

func analyzeBlock(b *tsdb.Block, limit int) {
	fmt.Printf("Block path: %s\n", b.Dir())
	meta := b.Meta()
	// Presume 1ms resolution that Prometheus uses.
	fmt.Printf("Duration: %s\n", (time.Duration(meta.MaxTime-meta.MinTime) * 1e6).String())
	fmt.Printf("Series: %d\n", meta.Stats.NumSeries)
	ir, err := b.Index()
	if err != nil {
		exitWithError(err)
	}
	defer ir.Close()

	allLabelNames, err := ir.LabelNames()
	if err != nil {
		exitWithError(err)
	}
	fmt.Printf("Label names: %d\n", len(allLabelNames))

	type postingInfo struct {
		key    string
		metric uint64
	}
	postingInfos := []postingInfo{}

	printInfo := func(postingInfos []postingInfo) {
		sort.Slice(postingInfos, func(i, j int) bool { return postingInfos[i].metric > postingInfos[j].metric })

		for i, pc := range postingInfos {
			fmt.Printf("%d %s\n", pc.metric, pc.key)
			if i >= limit {
				break
			}
		}
	}

	labelsUncovered := map[string]uint64{}
	labelpairsUncovered := map[string]uint64{}
	labelpairsCount := map[string]uint64{}
	entries := 0
	p, err := ir.Postings("", "") // The special all key.
	if err != nil {
		exitWithError(err)
	}
	lbls := labels.Labels{}
	chks := []chunkenc.Meta{}
	for p.Next() {
		if err = ir.Series(p.At(), &lbls, &chks); err != nil {
			exitWithError(err)
		}
		// Amount of the block time range not covered by this series.
		uncovered := uint64(meta.MaxTime-meta.MinTime) - uint64(chks[len(chks)-1].MaxTime-chks[0].MinTime)
		for _, lbl := range lbls {
			key := lbl.Name + "=" + lbl.Value
			labelsUncovered[lbl.Name] += uncovered
			labelpairsUncovered[key] += uncovered
			labelpairsCount[key]++
			entries++
		}
	}
	if p.Err() != nil {
		exitWithError(p.Err())
	}
	fmt.Printf("Postings (unique label pairs): %d\n", len(labelpairsUncovered))
	fmt.Printf("Postings entries (total label pairs): %d\n", entries)

	postingInfos = postingInfos[:0]
	for k, m := range labelpairsUncovered {
		postingInfos = append(postingInfos, postingInfo{k, uint64(float64(m) / float64(meta.MaxTime-meta.MinTime))})
	}

	fmt.Printf("\nLabel pairs most involved in churning:\n")
	printInfo(postingInfos)

	postingInfos = postingInfos[:0]
	for k, m := range labelsUncovered {
		postingInfos = append(postingInfos, postingInfo{k, uint64(float64(m) / float64(meta.MaxTime-meta.MinTime))})
	}

	fmt.Printf("\nLabel names most involved in churning:\n")
	printInfo(postingInfos)

	postingInfos = postingInfos[:0]
	for k, m := range labelpairsCount {
		postingInfos = append(postingInfos, postingInfo{k, m})
	}

	fmt.Printf("\nMost common label pairs:\n")
	printInfo(postingInfos)

	postingInfos = postingInfos[:0]
	for _, n := range allLabelNames {
		values, err := ir.LabelValues(n)
		if err != nil {
			exitWithError(err)
		}
		var cumulativeLength uint64

		for i := 0; i < values.Len(); i++ {
			value, _ := values.At(i)
			if err != nil {
				exitWithError(err)
			}
			for _, str := range value {
				cumulativeLength += uint64(len(str))
			}
		}

		postingInfos = append(postingInfos, postingInfo{n, cumulativeLength})
	}

	fmt.Printf("\nLabel names with highest cumulative label value length:\n")
	printInfo(postingInfos)

	postingInfos = postingInfos[:0]
	for _, n := range allLabelNames {
		lv, err := ir.LabelValues(n)
		if err != nil {
			exitWithError(err)
		}
		postingInfos = append(postingInfos, postingInfo{n, uint64(lv.Len())})
	}
	fmt.Printf("\nHighest cardinality labels:\n")
	printInfo(postingInfos)

	postingInfos = postingInfos[:0]
	lv, err := ir.LabelValues("__name__")
	if err != nil {
		exitWithError(err)
	}
	for i := 0; i < lv.Len(); i++ {
		names, err := lv.At(i)
		if err != nil {
			exitWithError(err)
		}
		for _, n := range names {
			postings, err := ir.Postings("__name__", n)
			if err != nil {
				exitWithError(err)
			}
			count := 0
			for postings.Next() {
				count++
			}
			if postings.Err() != nil {
				exitWithError(postings.Err())
			}
			postingInfos = append(postingInfos, postingInfo{n, uint64(count)})
		}
	}
	fmt.Printf("\nHighest cardinality metric names:\n")
	printInfo(postingInfos)
}

func dumpSamples(db *tsdb.DB, mint, maxt int64) {
	q, err := db.Querier(mint, maxt)
	if err != nil {
		exitWithError(err)
	}

	ss, err := q.Select(labels.NewMustRegexpMatcher("", ".*"))
	if err != nil {
		exitWithError(err)
	}

	for ss.Next() {
		series := ss.At()
		labels := series.Labels()
		it := series.Iterator()
		for it.Next() {
			ts, val := it.At()
			fmt.Printf("%s %g %d\n", labels, val, ts)
		}
		if it.Err() != nil {
			exitWithError(ss.Err())
		}
	}

	if ss.Err() != nil {
		exitWithError(ss.Err())
	}
}
