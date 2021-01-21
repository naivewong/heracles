package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	tsdb "github.com/naivewong/tsdb-group"
	"github.com/naivewong/tsdb-group/labels"
	"github.com/naivewong/tsdb-group/testutil"
	tsdb_origin "github.com/prometheus/tsdb"
	origin_labels "github.com/prometheus/tsdb/labels"
)

func LoadDevOpsSeries(lsets *[]origin_labels.Labels, queryLabels *[]origin_labels.Label, numSeries, numQueryLabels int, print bool) {
	start := time.Now()

	file, err := os.Open("./testdata/devops_series.json")
	testutil.Ok2(err)

	*lsets = (*lsets)[:0]
	*queryLabels = (*queryLabels)[:0]
	numSeries = (numSeries/101)*101 // Round

	scanner := bufio.NewScanner(file)
	for scanner.Scan() && len(*lsets) < numSeries {
		m := make(map[string]string)
		err = json.Unmarshal([]byte(scanner.Text()), &m)
		lset := origin_labels.FromMap(m)
		sort.Sort(lset)
		*lsets = append(*lsets, lset)
	}
	for i := 0; i < numQueryLabels; i++ {
		*queryLabels = append(*queryLabels, (*lsets)[i][0])
	}
	if print {
		fmt.Printf("> complete stage=db_load_labels total=%d duration=%f\n", len(*lsets), time.Since(start).Seconds())
	}
	file.Close()
}

func GroupLoadDevOpsSeries(lsets *[]labels.Labels, queryLabels *[]labels.Label, seriesPerGroup, numSeries, numQueryLabels int, print bool) {
	start := time.Now()

	file, err := os.Open("./testdata/devops_series.json")
	testutil.Ok2(err)

	*lsets = (*lsets)[:0]
	*queryLabels = (*queryLabels)[:0]
	numSeries = (numSeries/101)*101 // Round

	scanner := bufio.NewScanner(file)
	for scanner.Scan() && len(*lsets) < numSeries {
		m := make(map[string]string)
		err = json.Unmarshal([]byte(scanner.Text()), &m)
		lset := labels.FromMap(m)
		sort.Sort(lset)
		*lsets = append(*lsets, lset)
	}
	for i := 0; i < numQueryLabels; i++ {
		*queryLabels = append(*queryLabels, (*lsets)[i][0])
	}
	// Sort by group.
	g := &labels.ByGroup{Lsets: lsets, GSize: seriesPerGroup}
	sort.Sort(g)
	if print {
		fmt.Printf("> complete stage=group_db_load_labels total=%d duration=%f\n", len(*lsets), time.Since(start).Seconds())
	}
	file.Close()
}

// length of lsets must be divisible to seriesPerGroup.
func DBAppendRandom(db *tsdb_origin.DB, timeDelta, devMag int64, scrapeCount int, lsets []origin_labels.Labels) {
	refs := make([]uint64, len(lsets))
	for i := 0; i < len(refs); i++ {
		refs[i] = 0
	}

	start := time.Now()
	totalSamples := 0
	for j := 0; j < scrapeCount; j++ {
		t_ := timeDelta * int64(j) + int64(rand.Float64()*float64(devMag))
		app := db.Appender()
		for i := 0; i < len(lsets); i++ {
			if refs[i] == 0 {
				ref, err := app.Add(lsets[i], t_, rand.Float64())
				testutil.Ok2(err)
				refs[i] = ref
			} else {
				err := app.AddFast(refs[i], t_, rand.Float64())
				testutil.Ok2(err)
			}
		}
		totalSamples += len(lsets)
		err := app.Commit()
		testutil.Ok2(err)
	}
	end := time.Since(start)
	fmt.Printf("> complete stage=DBAppendRandom duration=%f\n", end.Seconds())
	fmt.Printf("  > total samples=%d\n", totalSamples)
	fmt.Printf("  > samples/sec=%f\n", float64(totalSamples)/end.Seconds())
}

func DBAppendTimeseries(db *tsdb_origin.DB, timeDelta, devMag int64, lsets []origin_labels.Labels) {
	refs := make([]uint64, len(lsets))
	for i := 0; i < len(refs); i++ {
		refs[i] = 0
	}

	file, err := os.Open("./testdata/bigdata/data50_12.txt")
	testutil.Ok2(err)
	scanner := bufio.NewScanner(file)

	start := time.Now()
	totalSamples := 0
	k := 0
	for scanner.Scan() {
		t_ := timeDelta * int64(k) + int64(rand.Float64()*float64(devMag))
		app := db.Appender()
		s := scanner.Text()
		data := strings.Split(s, ",")
		for i := 0; i < len(lsets); i++ {
			v, _ := strconv.ParseFloat(data[i], 64)
			if refs[i] == 0 {
				ref, err := app.Add(lsets[i], t_, v)
				testutil.Ok2(err)
				refs[i] = ref
			} else {
				err := app.AddFast(refs[i], t_, v)
				testutil.Ok2(err)
			}
		}
		totalSamples += len(lsets)
		err := app.Commit()
		testutil.Ok2(err)

		for i := 0; i < int(timeDelta)/1000-1; i++ {
			scanner.Scan()
		}
		k += 1
	}
	end := time.Since(start)
	fmt.Printf("> complete stage=DBAppendTimeseries duration=%f\n", end.Seconds())
	fmt.Printf("  > total samples=%d\n", totalSamples)
	fmt.Printf("  > samples/sec=%f\n", float64(totalSamples)/end.Seconds())
	file.Close()
}

// length of lsets must be divisible to seriesPerGroup.
func GroupDBAppendRandom(db *tsdb.DB, timeDelta, devMag int64, seriesPerGroup, scrapeCount int, lsets []labels.Labels) {
	refs := make([]uint64, (len(lsets)+seriesPerGroup-1)/seriesPerGroup)
	for i := 0; i < len(refs); i++ {
		refs[i] = 0
	}

	start := time.Now()
	totalSamples := 0
	for j := 0; j < scrapeCount; j++ {
		t_ := timeDelta * int64(j) + int64(rand.Float64()*float64(devMag))
		app := db.Appender()
		for i := 0; i < len(lsets); i += seriesPerGroup {
			tempVals := make([]float64, seriesPerGroup)
			for k := 0; k < len(tempVals); k++ {
				tempVals[k] = rand.Float64()
			}
			if refs[i/seriesPerGroup] == 0 {
				ref, err := app.AddGroup(lsets[i:i+seriesPerGroup], t_, tempVals)
				testutil.Ok2(err)
				refs[i/seriesPerGroup] = ref
			} else {
				err := app.AddGroupFast(refs[i/seriesPerGroup], t_, tempVals)
				testutil.Ok2(err)
			}
		}
		totalSamples += len(lsets)
		err := app.Commit()
		testutil.Ok2(err)
	}
	end := time.Since(start)
	fmt.Printf("> complete stage=GroupDBAppendRandom duration=%f\n", end.Seconds())
	fmt.Printf("  > total samples=%d\n", totalSamples)
	fmt.Printf("  > samples/sec=%f\n", float64(totalSamples)/end.Seconds())
}

func GroupDBAppendTimeseries(db *tsdb.DB, timeDelta, devMag int64, seriesPerGroup int, lsets []labels.Labels) {
	refs := make([]uint64, (len(lsets)+seriesPerGroup-1)/seriesPerGroup)
	for i := 0; i < len(refs); i++ {
		refs[i] = 0
	}

	file, err := os.Open("./testdata/bigdata/data50_12.txt")
	testutil.Ok2(err)
	scanner := bufio.NewScanner(file)

	start := time.Now()
	totalSamples := 0
	k := 0
	for scanner.Scan() {
		t_ := timeDelta * int64(k) + int64(rand.Float64()*float64(devMag))
		app := db.Appender()
		s := scanner.Text()
		data := strings.Split(s, ",")
		for i := 0; i < len(lsets); i += seriesPerGroup {
			tempData := make([]float64, 0, seriesPerGroup)
			for j := i; j < i + seriesPerGroup; j++ {
				v, _ := strconv.ParseFloat(data[j], 64)
				tempData = append(tempData, v)
			}
			if refs[i/seriesPerGroup] == 0 {
				ref, err := app.AddGroup(lsets[i:i+seriesPerGroup], t_, tempData)
				testutil.Ok2(err)
				refs[i/seriesPerGroup] = ref
			} else {
				err := app.AddGroupFast(refs[i/seriesPerGroup], t_, tempData)
				testutil.Ok2(err)
			}
		}
		totalSamples += len(lsets)
		err := app.Commit()
		testutil.Ok2(err)

		for i := 0; i < int(timeDelta)/1000-1; i++ {
			scanner.Scan()
		}
		k += 1
	}
	end := time.Since(start)
	fmt.Printf("> complete stage=GroupDBAppendTimeseries duration=%f\n", end.Seconds())
	fmt.Printf("  > total samples=%d\n", totalSamples)
	fmt.Printf("  > samples/sec=%f\n", float64(totalSamples)/end.Seconds())
	file.Close()
}

func DBQueryOrigin(db *tsdb_origin.DB, queryLabels []origin_labels.Label, timeDelta int64) {
	fmt.Println("-------- DBQueryOrigin ---------")
	starts := []int64{}
	ends := []int64{}
	for i := 3600*6*1000/timeDelta; i < 3600*12*1000/timeDelta; i += 300000/timeDelta {
		starts = append(starts, i * timeDelta)
		ends = append(ends, (i + 300000 / timeDelta) * timeDelta)
	}
	totalSamples := 0

	// Prepare matchers.
	matchers := []origin_labels.Matcher{}
	for _, l := range queryLabels {
		matchers = append(matchers, origin_labels.NewEqualMatcher(l.Name, l.Value))
	}

	start := time.Now()
	for i := 0; i < len(starts); i++ {
		q, err := db.Querier(starts[i], ends[i])
		testutil.Ok2(err)

		for _, matcher := range matchers {
			ss, err := q.Select(matcher)
			testutil.Ok2(err)

			for ss.Next() {
				it := ss.At().Iterator()
				for it.Next() {
					totalSamples += 1
				}
			}
		}
		q.Close()
	}
	fmt.Printf("> complete stage=DBQueryOrigin duration=%f\n", time.Since(start).Seconds())
	fmt.Printf("  > total samples=%d\n", totalSamples)
	fmt.Println("----------------------------------")
}

func DBQueryOriginFirstSample(db *tsdb_origin.DB, queryLabels []origin_labels.Label, timeDelta int64) {
	fmt.Println("-------- DBQueryOriginFirstSample ---------")
	starts := []int64{}
	ends := []int64{}
	for i := 3600*6*1000/timeDelta; i < 3600*12*1000/timeDelta; i += 300000/timeDelta {
		starts = append(starts, i * timeDelta)
		ends = append(ends, (i + 300000 / timeDelta) * timeDelta)
	}
	totalSamples := 0

	// Prepare matchers.
	matchers := []origin_labels.Matcher{}
	for _, l := range queryLabels {
		matchers = append(matchers, origin_labels.NewEqualMatcher(l.Name, l.Value))
	}

	start := time.Now()
	for i := 0; i < len(starts); i++ {
		q, err := db.Querier(starts[i], ends[i])
		testutil.Ok2(err)

		for _, matcher := range matchers {
			ss, err := q.Select(matcher)
			testutil.Ok2(err)

			for ss.Next() {
				it := ss.At().Iterator()
				if it.Next() {
					totalSamples += 1
				}
			}
		}
		q.Close()
	}
	fmt.Printf("> complete stage=DBQueryOriginFirstSample duration=%f\n", time.Since(start).Seconds())
	fmt.Printf("  > total samples=%d\n", totalSamples)
	fmt.Println("----------------------------------")
}

func DBQuerySameHost(db *tsdb_origin.DB, timeDelta int64) {
	fmt.Println("-------- DBQuerySameHost ---------")
	starts := []int64{}
	ends := []int64{}
	for i := 3600*6*1000/timeDelta; i < 3600*12*1000/timeDelta; i += 300000/timeDelta {
		starts = append(starts, i * timeDelta)
		ends = append(ends, (i + 300000 / timeDelta) * timeDelta)
	}
	totalSamples := 0

	// Prepare matchers.
	matchers := []origin_labels.Matcher{}
	for i := 0; i < 30; i++ {
		matchers = append(matchers, origin_labels.NewEqualMatcher("hostname", "host_"+strconv.Itoa(i)))
	}

	start := time.Now()
	for i := 0; i < len(starts); i++ {
		q, err := db.Querier(starts[i], ends[i])
		testutil.Ok2(err)

		for _, matcher := range matchers {
			ss, err := q.Select(matcher)
			testutil.Ok2(err)

			for ss.Next() {
				it := ss.At().Iterator()
				for it.Next() {
					totalSamples += 1
				}
			}
		}
		q.Close()
	}
	fmt.Printf("> complete stage=DBQuerySameHost duration=%f\n", time.Since(start).Seconds())
	fmt.Printf("  > total samples=%d\n", totalSamples)
	fmt.Println("----------------------------------")
}

func DBQuerySameHostFirstSample(db *tsdb_origin.DB, timeDelta int64) {
	fmt.Println("-------- DBQuerySameHostFirstSample ---------")
	starts := []int64{}
	ends := []int64{}
	for i := 3600*6*1000/timeDelta; i < 3600*12*1000/timeDelta; i += 300000/timeDelta {
		starts = append(starts, i * timeDelta)
		ends = append(ends, (i + 300000 / timeDelta) * timeDelta)
	}
	totalSamples := 0

	// Prepare matchers.
	matchers := []origin_labels.Matcher{}
	for i := 0; i < 30; i++ {
		matchers = append(matchers, origin_labels.NewEqualMatcher("hostname", "host_"+strconv.Itoa(i)))
	}

	start := time.Now()
	for i := 0; i < len(starts); i++ {
		q, err := db.Querier(starts[i], ends[i])
		testutil.Ok2(err)

		for _, matcher := range matchers {
			ss, err := q.Select(matcher)
			testutil.Ok2(err)

			for ss.Next() {
				it := ss.At().Iterator()
				if it.Next() {
					totalSamples += 1
				}
			}
		}
		q.Close()
	}
	fmt.Printf("> complete stage=DBQuerySameHostFirstSample duration=%f\n", time.Since(start).Seconds())
	fmt.Printf("  > total samples=%d\n", totalSamples)
	fmt.Println("----------------------------------")
}

func GroupDBQueryOrigin(db *tsdb.DB, queryLabels []labels.Label, timeDelta int64) {
	fmt.Println("-------- GroupDBQueryOrigin ---------")
	starts := []int64{}
	ends := []int64{}
	for i := 3600*6*1000/timeDelta; i < 3600*12*1000/timeDelta; i += 300000/timeDelta {
		starts = append(starts, i * timeDelta)
		ends = append(ends, (i + 300000 / timeDelta) * timeDelta)
	}
	totalSamples := 0

	// Prepare matchers.
	matchers := []labels.Matcher{}
	for _, l := range queryLabels {
		matchers = append(matchers, labels.NewEqualMatcher(l.Name, l.Value))
	}

	start := time.Now()
	for i := 0; i < len(starts); i++ {
		q, err := db.Querier(starts[i], ends[i])
		testutil.Ok2(err)

		for _, matcher := range matchers {
			ss, err := q.Select(matcher)
			testutil.Ok2(err)

			for ss.Next() {
				it := ss.At().Iterator()
				for it.Next() {
					totalSamples += 1
				}
			}
		}
		q.Close()
	}
	fmt.Printf("> complete stage=GroupDBQueryOrigin duration=%f\n", time.Since(start).Seconds())
	fmt.Printf("  > total samples=%d\n", totalSamples)
	fmt.Println("----------------------------------")
}

func GroupDBQueryOriginFirstSample(db *tsdb.DB, queryLabels []labels.Label, timeDelta int64) {
	fmt.Println("-------- GroupDBQueryOriginFirstSample ---------")
	starts := []int64{}
	ends := []int64{}
	for i := 3600*6*1000/timeDelta; i < 3600*12*1000/timeDelta; i += 300000/timeDelta {
		starts = append(starts, i * timeDelta)
		ends = append(ends, (i + 300000 / timeDelta) * timeDelta)
	}
	totalSamples := 0

	// Prepare matchers.
	matchers := []labels.Matcher{}
	for _, l := range queryLabels {
		matchers = append(matchers, labels.NewEqualMatcher(l.Name, l.Value))
	}

	start := time.Now()
	for i := 0; i < len(starts); i++ {
		q, err := db.Querier(starts[i], ends[i])
		testutil.Ok2(err)

		for _, matcher := range matchers {
			ss, err := q.Select(matcher)
			testutil.Ok2(err)

			for ss.Next() {
				it := ss.At().Iterator()
				if it.Next() {
					totalSamples += 1
				}
			}
		}
		q.Close()
	}
	fmt.Printf("> complete stage=GroupDBQueryOriginFirstSample duration=%f\n", time.Since(start).Seconds())
	fmt.Printf("  > total samples=%d\n", totalSamples)
	fmt.Println("----------------------------------")
}

func GroupDBQuerySameHost(db *tsdb.DB, timeDelta int64) {
	fmt.Println("-------- GroupDBQuerySameHost ---------")
	starts := []int64{}
	ends := []int64{}
	for i := 3600*6*1000/timeDelta; i < 3600*12*1000/timeDelta; i += 300000/timeDelta {
		starts = append(starts, i * timeDelta)
		ends = append(ends, (i + 300000 / timeDelta) * timeDelta)
	}
	totalSamples := 0

	// Prepare matchers.
	matchers := []labels.Matcher{}
	for i := 0; i < 30; i++ {
		matchers = append(matchers, labels.NewEqualMatcher("hostname", "host_"+strconv.Itoa(i)))
	}

	start := time.Now()
	for i := 0; i < len(starts); i++ {
		q, err := db.Querier(starts[i], ends[i])
		testutil.Ok2(err)

		for _, matcher := range matchers {
			ss, err := q.Select(matcher)
			testutil.Ok2(err)

			for ss.Next() {
				it := ss.At().Iterator()
				for it.Next() {
					totalSamples += 1
				}
			}
		}
		q.Close()
	}
	fmt.Printf("> complete stage=GroupDBQuerySameHost duration=%f\n", time.Since(start).Seconds())
	fmt.Printf("  > total samples=%d\n", totalSamples)
	fmt.Println("----------------------------------")
}

func GroupDBQuerySameHostFirstSample(db *tsdb.DB, timeDelta int64) {
	fmt.Println("-------- GroupDBQuerySameHostFirstSample ---------")
	starts := []int64{}
	ends := []int64{}
	for i := 3600*6*1000/timeDelta; i < 3600*12*1000/timeDelta; i += 300000/timeDelta {
		starts = append(starts, i * timeDelta)
		ends = append(ends, (i + 300000 / timeDelta) * timeDelta)
	}
	totalSamples := 0

	// Prepare matchers.
	matchers := []labels.Matcher{}
	for i := 0; i < 30; i++ {
		matchers = append(matchers, labels.NewEqualMatcher("hostname", "host_"+strconv.Itoa(i)))
	}

	start := time.Now()
	for i := 0; i < len(starts); i++ {
		q, err := db.Querier(starts[i], ends[i])
		testutil.Ok2(err)

		for _, matcher := range matchers {
			ss, err := q.Select(matcher)
			testutil.Ok2(err)

			for ss.Next() {
				it := ss.At().Iterator()
				if it.Next() {
					totalSamples += 1
				}
			}
		}
		q.Close()
	}
	fmt.Printf("> complete stage=GroupDBQuerySameHostFirstSample duration=%f\n", time.Since(start).Seconds())
	fmt.Printf("  > total samples=%d\n", totalSamples)
	fmt.Println("----------------------------------")
}

func DBBenchRandom(timeDelta, devMag int64, typ, numSeries, scrapeCount, numQueryLabels int) {
	dbPath := "db_bench_random/" + strconv.Itoa(int(timeDelta))
	os.RemoveAll(dbPath)
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	db, err := tsdb_origin.Open(dbPath, logger, nil, &tsdb_origin.Options{
		WALSegmentSize:         -1,
		RetentionDuration:      15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds
		BlockRanges:            tsdb.ExponentialBlockRanges(int64(2*time.Hour)/1e6, 3, 5),
		NoLockfile:             false,
		AllowOverlappingBlocks: false,
	})
	testutil.Ok2(err)

	lsets := []origin_labels.Labels{}
	queryLabels := []origin_labels.Label{}

	LoadDevOpsSeries(&lsets, &queryLabels, numSeries, numQueryLabels, true)
	DBAppendRandom(db, timeDelta, devMag, scrapeCount, lsets)
	time.Sleep(time.Second*25)

	switch typ {
	case 1:
		DBQueryOrigin(db, queryLabels, timeDelta)
	case 2:
		DBQueryOriginFirstSample(db, queryLabels, timeDelta)
	case 3:
		DBQuerySameHost(db, timeDelta)
	case 4:
		DBQuerySameHostFirstSample(db, timeDelta)
	default:
		DBQueryOrigin(db, queryLabels, timeDelta)
		time.Sleep(time.Second*10)
		DBQueryOriginFirstSample(db, queryLabels, timeDelta)
		time.Sleep(time.Second*10)
		DBQuerySameHost(db, timeDelta)
		time.Sleep(time.Second*10)
		DBQuerySameHostFirstSample(db, timeDelta)
	}
	db.Close()
}

func DBBenchTimeseries(timeDelta, devMag int64, typ, numSeries, numQueryLabels int) {
	dbPath := "db_bench_timeseries/" + strconv.Itoa(int(timeDelta))
	os.RemoveAll(dbPath)
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	db, err := tsdb_origin.Open(dbPath, logger, nil, &tsdb_origin.Options{
		WALSegmentSize:         -1,
		RetentionDuration:      15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds
		BlockRanges:            tsdb.ExponentialBlockRanges(int64(2*time.Hour)/1e6, 3, 5),
		NoLockfile:             false,
		AllowOverlappingBlocks: false,
	})
	testutil.Ok2(err)

	lsets := []origin_labels.Labels{}
	queryLabels := []origin_labels.Label{}

	LoadDevOpsSeries(&lsets, &queryLabels, numSeries, numQueryLabels, true)
	DBAppendTimeseries(db, timeDelta, devMag, lsets)
	time.Sleep(time.Second*25)

	switch typ {
	case 1:
		DBQueryOrigin(db, queryLabels, timeDelta)
	case 2:
		DBQueryOriginFirstSample(db, queryLabels, timeDelta)
	case 3:
		DBQuerySameHost(db, timeDelta)
	case 4:
		DBQuerySameHostFirstSample(db, timeDelta)
	default:
		DBQueryOrigin(db, queryLabels, timeDelta)
		time.Sleep(time.Second*10)
		DBQueryOriginFirstSample(db, queryLabels, timeDelta)
		time.Sleep(time.Second*10)
		DBQuerySameHost(db, timeDelta)
		time.Sleep(time.Second*10)
		DBQuerySameHostFirstSample(db, timeDelta)
	}
	db.Close()
}

func GroupDBBenchRandom(timeDelta, devMag int64, typ, seriesPerGroup, numSeries, scrapeCount, numQueryLabels int) {
	dbPath := "group_db_bench_random/" + strconv.Itoa(int(timeDelta))
	os.RemoveAll(dbPath)
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	db, err := tsdb.Open(dbPath, logger, nil, &tsdb.Options{
		WALSegmentSize:         -1,
		RetentionDuration:      15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds
		BlockRanges:            tsdb.ExponentialBlockRanges(int64(2*time.Hour)/1e6, 3, 5),
		NoLockfile:             false,
		AllowOverlappingBlocks: false,
		WALCompression:         false,
	})
	testutil.Ok2(err)

	lsets := []labels.Labels{}
	queryLabels := []labels.Label{}

	GroupLoadDevOpsSeries(&lsets, &queryLabels, seriesPerGroup, numSeries, numQueryLabels, true)
	GroupDBAppendRandom(db, timeDelta, devMag, seriesPerGroup, scrapeCount, lsets)
	time.Sleep(time.Second*90)

	switch typ {
	case 1:
		GroupDBQueryOrigin(db, queryLabels, timeDelta)
	case 2:
		GroupDBQueryOriginFirstSample(db, queryLabels, timeDelta)
	case 3:
		GroupDBQuerySameHost(db, timeDelta)
	case 4:
		GroupDBQuerySameHostFirstSample(db, timeDelta)
	default:
		GroupDBQueryOrigin(db, queryLabels, timeDelta)
		time.Sleep(time.Second*10)
		GroupDBQueryOriginFirstSample(db, queryLabels, timeDelta)
		time.Sleep(time.Second*10)
		GroupDBQuerySameHost(db, timeDelta)
		time.Sleep(time.Second*10)
		GroupDBQuerySameHostFirstSample(db, timeDelta)
	}
	db.Close()
}

func GroupDBBenchTimeseries(timeDelta, devMag int64, typ, seriesPerGroup, numSeries, numQueryLabels int) {
	dbPath := "group_db_bench_timeseries/" + strconv.Itoa(int(timeDelta))
	os.RemoveAll(dbPath)
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	db, err := tsdb.Open(dbPath, logger, nil, &tsdb.Options{
		WALSegmentSize:         -1,
		RetentionDuration:      15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds
		BlockRanges:            tsdb.ExponentialBlockRanges(int64(2*time.Hour)/1e6, 3, 5),
		NoLockfile:             false,
		AllowOverlappingBlocks: false,
		WALCompression:         false,
	})
	testutil.Ok2(err)

	lsets := []labels.Labels{}
	queryLabels := []labels.Label{}

	GroupLoadDevOpsSeries(&lsets, &queryLabels, seriesPerGroup, numSeries, numQueryLabels, true)
	GroupDBAppendTimeseries(db, timeDelta, devMag, seriesPerGroup, lsets)
	time.Sleep(time.Second*90)

	switch typ {
	case 1:
		GroupDBQueryOrigin(db, queryLabels, timeDelta)
	case 2:
		GroupDBQueryOriginFirstSample(db, queryLabels, timeDelta)
	case 3:
		GroupDBQuerySameHost(db, timeDelta)
	case 4:
		GroupDBQuerySameHostFirstSample(db, timeDelta)
	default:
		GroupDBQueryOrigin(db, queryLabels, timeDelta)
		time.Sleep(time.Second*10)
		GroupDBQueryOriginFirstSample(db, queryLabels, timeDelta)
		time.Sleep(time.Second*10)
		GroupDBQuerySameHost(db, timeDelta)
		time.Sleep(time.Second*10)
		GroupDBQuerySameHostFirstSample(db, timeDelta)
	}
	db.Close()
}

func main() {
	timeDeltas := []int64{1000, 5000, 10000, 15000, 30000}
	// timeDeltas := []int64{15000}
	for i := 1; i <= 4; i++ {
		for _, timeDelta := range timeDeltas {
			fmt.Println("------- time delta", timeDelta)
			DBBenchRandom(timeDelta, 10, i, 5000, 3600*12*1000/int(timeDelta), 200)
			DBBenchTimeseries(timeDelta, 10, i, 5000, 200)
			GroupDBBenchRandom(timeDelta, 10, i, 101, 5000, 3600*12*1000/int(timeDelta), 200)
			GroupDBBenchTimeseries(timeDelta, 10, i, 101, 5000, 200)
		}
	}
}