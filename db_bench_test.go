package tsdb

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/naivewong/tsdb-group/labels"
	"github.com/naivewong/tsdb-group/testutil"
	tsdb_origin "github.com/prometheus/tsdb"
	origin_labels "github.com/prometheus/tsdb/labels"
)

var numNEData int
var NumSeries = 5050
var OriginSleep = 20
var GroupSleep = 50

func init() {
	files, err := ioutil.ReadDir("./testdata/bigdata/node_exporter/")
	if err != nil {
		fmt.Println("Error list folder ./testdata/bigdata/node_exporter/")
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

func LoadDevOpsSeries(t testing.TB, lsets *[]origin_labels.Labels, queryLabels *[]origin_labels.Label, numSeries, numQueryLabels int, print bool) {
	start := time.Now()

	file, err := os.Open("./testdata/devops_series.json")
	testutil.Ok(t, err)

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

func LoadNodeExporterSeries(t testing.TB, lsets *[]origin_labels.Labels, queryLabels *[]origin_labels.Label, numSeries, numQueryLabels, total int, print bool) {
	*lsets = (*lsets)[:0]
	*queryLabels = (*queryLabels)[:0]
	numSeries = (numSeries/101)*101 // Round
	start := time.Now()

	numFiles := total / numSeries

	for j := 0; j < numFiles; j++ {
		file, err := os.Open("./testdata/bigdata/node_exporter/timeseries.json")
		testutil.Ok(t, err)

		scanner := bufio.NewScanner(file)
		count := 0
		for scanner.Scan() && count < numSeries {
			m := make(map[string]string)
			err = json.Unmarshal([]byte(scanner.Text()), &m)
			m["instance"] = fmt.Sprintf("pc9%06d:9100", j)
			lset := origin_labels.FromMap(m)
			sort.Sort(lset)
			*lsets = append(*lsets, lset)
			count++
		}
		file.Close()
	}
	for i := 0; i < numQueryLabels; i++ {
		*queryLabels = append(*queryLabels, (*lsets)[i][0])
	}
	if print {
		fmt.Printf("> complete stage=db_load_ne_labels total=%d duration=%f\n", len(*lsets), time.Since(start).Seconds())
	}
}

func GroupLoadDevOpsSeries(t testing.TB, lsets *[]labels.Labels, queryLabels *[]labels.Label, seriesPerGroup, numSeries, numQueryLabels int, print bool) {
	start := time.Now()

	file, err := os.Open("./testdata/devops_series.json")
	testutil.Ok(t, err)

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

func GroupLoadNodeExporterSeries(t testing.TB, lsets *[]labels.Labels, queryLabels *[]labels.Label, seriesPerGroup, numSeries, numQueryLabels, total int, print bool) {
	*lsets = (*lsets)[:0]
	*queryLabels = (*queryLabels)[:0]
	numSeries = (numSeries/101)*101 // Round
	start := time.Now()

	numFiles := total / numSeries

	for j := 0; j < numFiles; j++ { 
		file, err := os.Open("./testdata/bigdata/node_exporter/timeseries.json")
		testutil.Ok(t, err)

		scanner := bufio.NewScanner(file)
		count := 0
		for scanner.Scan() && count < numSeries {
			m := make(map[string]string)
			err = json.Unmarshal([]byte(scanner.Text()), &m)
			m["instance"] = fmt.Sprintf("pc9%06d:9100", j)
			lset := labels.FromMap(m)
			sort.Sort(lset)
			*lsets = append(*lsets, lset)
			count++
		}
		file.Close()
	}
	for i := 0; i < numQueryLabels; i++ {
		*queryLabels = append(*queryLabels, (*lsets)[i][0])
	}
	// Sort by group.
	g := &labels.ByGroup{Lsets: lsets, GSize: seriesPerGroup}
	sort.Sort(g)
	if print {
		fmt.Printf("> complete stage=group_db_load_ne_labels total=%d duration=%f\n", len(*lsets), time.Since(start).Seconds())
	}
}

// length of lsets must be divisible to seriesPerGroup.
func DBAppendRandom(t testing.TB, db *tsdb_origin.DB, timeDelta, devMag int64, scrapeCount int, lsets []origin_labels.Labels) {
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
	end := time.Since(start)
	fmt.Printf("> complete stage=DBAppendRandom duration=%f\n", end.Seconds())
	fmt.Printf("  > total samples=%d\n", totalSamples)
	fmt.Printf("  > samples/sec=%f\n", float64(totalSamples)/end.Seconds())
}

func DBAppendCounter(t testing.TB, db *tsdb_origin.DB, timeDelta, devMag int64, scrapeCount int, lsets []origin_labels.Labels) {
	refs := make([]uint64, len(lsets))
	for i := 0; i < len(refs); i++ {
		refs[i] = 0
	}

	counter := float64(123456789)

	start := time.Now()
	totalSamples := 0
	for j := 0; j < scrapeCount; j++ {
		t_ := timeDelta * int64(j) + int64(rand.Float64()*float64(devMag))
		app := db.Appender()
		for i := 0; i < len(lsets); i++ {
			if refs[i] == 0 {
				ref, err := app.Add(lsets[i], t_, counter)
				testutil.Ok(t, err)
				refs[i] = ref
			} else {
				err := app.AddFast(refs[i], t_, counter)
				testutil.Ok(t, err)
			}
		}
		totalSamples += len(lsets)
		err := app.Commit()
		testutil.Ok(t, err)

		counter += 1000.0
	}
	end := time.Since(start)
	fmt.Printf("> complete stage=DBAppendCounter duration=%f\n", end.Seconds())
	fmt.Printf("  > total samples=%d\n", totalSamples)
	fmt.Printf("  > samples/sec=%f\n", float64(totalSamples)/end.Seconds())
}

func DBAppendTimeseries(t testing.TB, db *tsdb_origin.DB, timeDelta, devMag int64, lsets []origin_labels.Labels) {
	refs := make([]uint64, len(lsets))
	for i := 0; i < len(refs); i++ {
		refs[i] = 0
	}

	file, err := os.Open("./testdata/bigdata/data50_12.txt")
	testutil.Ok(t, err)
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

func DBAppendNodeExporterTimeseries(t testing.TB, db *tsdb_origin.DB, timeDelta, devMag int64, lsets []origin_labels.Labels) {
	refs := make([]uint64, len(lsets))
	for i := 0; i < len(refs); i++ {
		refs[i] = 0
	}

	start := time.Now()

	totalSamples := 0
	filesToOpen := numNEData
	if filesToOpen > len(lsets) / 505 {
		filesToOpen = len(lsets) / 505
	}
	scanners := make([]*bufio.Scanner, len(lsets) / 505)
	files := make([]*os.File, len(lsets) / 505)
	for j := 0; j < filesToOpen; j++ {
		file, err := os.Open("./testdata/bigdata/node_exporter/data" + strconv.Itoa(j))
		testutil.Ok(t, err)
		scanners[j] = bufio.NewScanner(file)
		files[j] = file
	}
	for j := filesToOpen; j < len(lsets) / 505; j++ {
		file, err := os.Open("./testdata/bigdata/node_exporter/data" + strconv.Itoa(j - filesToOpen))
		testutil.Ok(t, err)
		scanners[j] = bufio.NewScanner(file)
		files[j] = file
	}
		
	k := 0
	for scanners[0].Scan() {
		idx := 0
		t_ := timeDelta * int64(k) + int64(rand.Float64()*float64(devMag))
		app := db.Appender()
		{
			s := scanners[0].Text()
			data := strings.Split(s, ",")
			for i := idx; i < idx+505; i++ {
				v, _ := strconv.ParseFloat(data[i-idx], 64)
				if refs[i] == 0 {
					ref, err := app.Add(lsets[i], t_, v)
					testutil.Ok(t, err)
					refs[i] = ref
				} else {
					err := app.AddFast(refs[i], t_, v)
					testutil.Ok(t, err)
				}
			}
			totalSamples += 505

			for i := 0; i < int(timeDelta)/5000-1; i++ {
				scanners[0].Scan()
			}
			idx += 505
		}
		for sc := 1; sc < len(lsets) / 505; sc++ {
			if idx >= len(lsets) {
				break
			}
			end := idx + 505
			if end > len(lsets) {
				end = len(lsets)
			}
			scanners[sc].Scan()
			s := scanners[sc].Text()
			data := strings.Split(s, ",")
			for i := idx; i < end; i++ {
				v, _ := strconv.ParseFloat(data[i-idx], 64)
				if refs[i] == 0 {
					ref, err := app.Add(lsets[i], t_, v)
					testutil.Ok(t, err)
					refs[i] = ref
				} else {
					err := app.AddFast(refs[i], t_, v)
					testutil.Ok(t, err)
				}
			}
			totalSamples += end - idx

			for i := 0; i < int(timeDelta)/5000-1; i++ {
				scanners[sc].Scan()
			}
			idx = end
		}
		err := app.Commit()
		testutil.Ok(t, err)
		k += 1
	}

	end := time.Since(start)
	fmt.Printf("> complete stage=DBAppendNodeExporterTimeseries duration=%f\n", end.Seconds())
	fmt.Printf("  > total samples=%d\n", totalSamples)
	fmt.Printf("  > samples/sec=%f\n", float64(totalSamples)/end.Seconds())

	for _, f := range files {
		f.Close()
	}
}

// length of lsets must be divisible to seriesPerGroup.
func GroupDBAppendRandom(t testing.TB, db *DB, timeDelta, devMag int64, seriesPerGroup, scrapeCount int, lsets []labels.Labels) {
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
				testutil.Ok(t, err)
				refs[i/seriesPerGroup] = ref
			} else {
				err := app.AddGroupFast(refs[i/seriesPerGroup], t_, tempVals)
				testutil.Ok(t, err)
			}
		}
		totalSamples += len(lsets)
		err := app.Commit()
		testutil.Ok(t, err)
	}
	end := time.Since(start)
	fmt.Printf("> complete stage=GroupDBAppendRandom duration=%f\n", end.Seconds())
	fmt.Printf("  > total samples=%d\n", totalSamples)
	fmt.Printf("  > samples/sec=%f\n", float64(totalSamples)/end.Seconds())
}

func GroupDBAppendCounter(t testing.TB, db *DB, timeDelta, devMag int64, seriesPerGroup, scrapeCount int, lsets []labels.Labels) {
	refs := make([]uint64, (len(lsets)+seriesPerGroup-1)/seriesPerGroup)
	for i := 0; i < len(refs); i++ {
		refs[i] = 0
	}

	tempVals := make([]float64, seriesPerGroup)
	for k := 0; k < len(tempVals); k++ {
		tempVals[k] = float64(123456789)
	}

	start := time.Now()
	totalSamples := 0
	for j := 0; j < scrapeCount; j++ {
		t_ := timeDelta * int64(j) + int64(rand.Float64()*float64(devMag))
		app := db.Appender()
		for i := 0; i < len(lsets); i += seriesPerGroup {
			if refs[i/seriesPerGroup] == 0 {
				ref, err := app.AddGroup(lsets[i:i+seriesPerGroup], t_, tempVals)
				testutil.Ok(t, err)
				refs[i/seriesPerGroup] = ref
			} else {
				err := app.AddGroupFast(refs[i/seriesPerGroup], t_, tempVals)
				testutil.Ok(t, err)
			}
		}
		totalSamples += len(lsets)
		err := app.Commit()
		testutil.Ok(t, err)

		for k := 0; k < len(tempVals); k++ {
			tempVals[k] = rand.Float64()
		}
	}
	end := time.Since(start)
	fmt.Printf("> complete stage=GroupDBAppendCounter duration=%f\n", end.Seconds())
	fmt.Printf("  > total samples=%d\n", totalSamples)
	fmt.Printf("  > samples/sec=%f\n", float64(totalSamples)/end.Seconds())
}

func GroupDBAppendTimeseries(t testing.TB, db *DB, timeDelta, devMag int64, seriesPerGroup int, lsets []labels.Labels) {
	refs := make([]uint64, (len(lsets)+seriesPerGroup-1)/seriesPerGroup)
	for i := 0; i < len(refs); i++ {
		refs[i] = 0
	}

	file, err := os.Open("./testdata/bigdata/data50_12.txt")
	testutil.Ok(t, err)
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
				testutil.Ok(t, err)
				refs[i/seriesPerGroup] = ref
			} else {
				err := app.AddGroupFast(refs[i/seriesPerGroup], t_, tempData)
				testutil.Ok(t, err)
			}
		}
		totalSamples += len(lsets)
		err := app.Commit()
		testutil.Ok(t, err)

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

func GroupDBAppendNodeExporterTimeseries(t testing.TB, db *DB, timeDelta, devMag int64, seriesPerGroup int, lsets []labels.Labels) {
	refs := make([]uint64, (len(lsets)+seriesPerGroup-1)/seriesPerGroup)
	for i := 0; i < len(refs); i++ {
		refs[i] = 0
	}

	start := time.Now()
	totalSamples := 0
	filesToOpen := numNEData
	if filesToOpen > len(lsets) / 505 {
		filesToOpen = len(lsets) / 505
	}
	scanners := make([]*bufio.Scanner, len(lsets) / 505)
	files := make([]*os.File, len(lsets) / 505)
	for j := 0; j < filesToOpen; j++ {
		file, err := os.Open("./testdata/bigdata/node_exporter/data" + strconv.Itoa(j))
		testutil.Ok(t, err)
		scanners[j] = bufio.NewScanner(file)
		files[j] = file
	}
	for j := filesToOpen; j < len(lsets) / 505; j++ {
		file, err := os.Open("./testdata/bigdata/node_exporter/data" + strconv.Itoa(j - filesToOpen))
		testutil.Ok(t, err)
		scanners[j] = bufio.NewScanner(file)
		files[j] = file
	}

	k := 0
	for scanners[0].Scan() {
		idx := 0
		t_ := timeDelta * int64(k) + int64(rand.Float64()*float64(devMag))
		app := db.Appender()
		{
			s := scanners[0].Text()
			data := strings.Split(s, ",")
			for i := idx; i < idx+505; i += seriesPerGroup {
				tempData := make([]float64, 0, seriesPerGroup)
				for j := i; j < i + seriesPerGroup; j++ {
					v, _ := strconv.ParseFloat(data[j-idx], 64)
					tempData = append(tempData, v)
				}
				if refs[i/seriesPerGroup] == 0 {
					ref, err := app.AddGroup(lsets[i:i+seriesPerGroup], t_, tempData)
					testutil.Ok(t, err)
					refs[i/seriesPerGroup] = ref
				} else {
					err := app.AddGroupFast(refs[i/seriesPerGroup], t_, tempData)
					testutil.Ok(t, err)
				}
			}
			totalSamples += 505

			for i := 0; i < int(timeDelta)/5000-1; i++ {
				scanners[0].Scan()
			}
			idx += 505
		}
		for sc := 1; sc < len(lsets) / 505; sc++ {
			if idx >= len(lsets) {
				break
			}
			end := idx + 505
			if end > len(lsets) {
				end = len(lsets)
			}
			scanners[sc].Scan()
			s := scanners[sc].Text()
			data := strings.Split(s, ",")
			for i := idx; i < end; i += seriesPerGroup {
				tempData := make([]float64, 0, seriesPerGroup)
				for j := i; j < i + seriesPerGroup; j++ {
					v, _ := strconv.ParseFloat(data[j-idx], 64)
					tempData = append(tempData, v)
				}
				if refs[i/seriesPerGroup] == 0 {
					ref, err := app.AddGroup(lsets[i:i+seriesPerGroup], t_, tempData)
					testutil.Ok(t, err)
					refs[i/seriesPerGroup] = ref
				} else {
					err := app.AddGroupFast(refs[i/seriesPerGroup], t_, tempData)
					testutil.Ok(t, err)
				}
			}
			totalSamples += end - idx

			for i := 0; i < int(timeDelta)/5000-1; i++ {
				scanners[sc].Scan()
			}
			idx = end
		}
		k += 1
		err := app.Commit()
		testutil.Ok(t, err)
	}
	end := time.Since(start)
	fmt.Printf("> complete stage=GroupDBAppendNodeExporterTimeseries duration=%f\n", end.Seconds())
	fmt.Printf("  > total samples=%d\n", totalSamples)
	fmt.Printf("  > samples/sec=%f\n", float64(totalSamples)/end.Seconds())

	for _, f := range files {
		f.Close()
	}
}

func DBQueryOrigin(t testing.TB, db *tsdb_origin.DB, queryLabels []origin_labels.Label, timeDelta int64) {
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
	fmt.Printf("> complete stage=DBQueryOrigin duration=%f\n", time.Since(start).Seconds())
	fmt.Printf("  > total samples=%d\n", totalSamples)
	fmt.Println("----------------------------------")
}

func DBQueryOriginFirstSample(t testing.TB, db *tsdb_origin.DB, queryLabels []origin_labels.Label, timeDelta int64) {
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
		testutil.Ok(t, err)

		for _, matcher := range matchers {
			ss, err := q.Select(matcher)
			testutil.Ok(t, err)

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

func DBQuerySameHost(t testing.TB, db *tsdb_origin.DB, timeDelta int64) {
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
	fmt.Printf("> complete stage=DBQuerySameHost duration=%f\n", time.Since(start).Seconds())
	fmt.Printf("  > total samples=%d\n", totalSamples)
	fmt.Println("----------------------------------")
}

func DBQuerySameHostFirstSample(t testing.TB, db *tsdb_origin.DB, timeDelta int64) {
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
		testutil.Ok(t, err)

		for _, matcher := range matchers {
			ss, err := q.Select(matcher)
			testutil.Ok(t, err)

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

func GroupDBQueryOrigin(t testing.TB, db *DB, queryLabels []labels.Label, timeDelta int64) {
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
	fmt.Printf("> complete stage=GroupDBQueryOrigin duration=%f\n", time.Since(start).Seconds())
	fmt.Printf("  > total samples=%d\n", totalSamples)
	fmt.Println("----------------------------------")
}

func GroupDBQueryOriginFirstSample(t testing.TB, db *DB, queryLabels []labels.Label, timeDelta int64) {
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
		testutil.Ok(t, err)

		for _, matcher := range matchers {
			ss, err := q.Select(matcher)
			testutil.Ok(t, err)

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

func GroupDBQuerySameHost(t testing.TB, db *DB, timeDelta int64) {
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
	fmt.Printf("> complete stage=GroupDBQuerySameHost duration=%f\n", time.Since(start).Seconds())
	fmt.Printf("  > total samples=%d\n", totalSamples)
	fmt.Println("----------------------------------")
}

func GroupDBQuerySameHostFirstSample(t testing.TB, db *DB, timeDelta int64) {
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
		testutil.Ok(t, err)

		for _, matcher := range matchers {
			ss, err := q.Select(matcher)
			testutil.Ok(t, err)

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

func DBBenchRandom(t testing.TB, timeDelta, devMag int64, typ, numSeries, scrapeCount, numQueryLabels int) {
	dbPath := "db_bench_random/" + strconv.Itoa(int(timeDelta))
	os.RemoveAll(dbPath)
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	db, err := tsdb_origin.Open(dbPath, logger, nil, &tsdb_origin.Options{
		WALSegmentSize:         -1,
		RetentionDuration:      15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds
		BlockRanges:            ExponentialBlockRanges(int64(2*time.Hour)/1e6, 3, 5),
		NoLockfile:             false,
		AllowOverlappingBlocks: false,
	})
	testutil.Ok(t, err)

	lsets := []origin_labels.Labels{}
	queryLabels := []origin_labels.Label{}

	LoadDevOpsSeries(t, &lsets, &queryLabels, numSeries, numQueryLabels, true)
	DBAppendRandom(t, db, timeDelta, devMag, scrapeCount, lsets)
	time.Sleep(time.Second*time.Duration(OriginSleep))

	switch typ {
	case 1:
		DBQueryOrigin(t, db, queryLabels, timeDelta)
	case 2:
		DBQueryOriginFirstSample(t, db, queryLabels, timeDelta)
	case 3:
		DBQuerySameHost(t, db, timeDelta)
	case 4:
		DBQuerySameHostFirstSample(t, db, timeDelta)
	default:
		DBQueryOrigin(t, db, queryLabels, timeDelta)
		time.Sleep(time.Second*10)
		DBQueryOriginFirstSample(t, db, queryLabels, timeDelta)
		time.Sleep(time.Second*10)
		DBQuerySameHost(t, db, timeDelta)
		time.Sleep(time.Second*10)
		DBQuerySameHostFirstSample(t, db, timeDelta)
	}
	db.Close()
}

func DBBenchTimeseries(t testing.TB, timeDelta, devMag int64, typ, numSeries, numQueryLabels int) {
	dbPath := "db_bench_timeseries/" + strconv.Itoa(int(timeDelta))
	os.RemoveAll(dbPath)
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	db, err := tsdb_origin.Open(dbPath, logger, nil, &tsdb_origin.Options{
		WALSegmentSize:         -1,
		RetentionDuration:      15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds
		BlockRanges:            ExponentialBlockRanges(int64(2*time.Hour)/1e6, 3, 5),
		NoLockfile:             false,
		AllowOverlappingBlocks: false,
	})
	testutil.Ok(t, err)

	lsets := []origin_labels.Labels{}
	queryLabels := []origin_labels.Label{}

	LoadDevOpsSeries(t, &lsets, &queryLabels, numSeries, numQueryLabels, true)
	DBAppendTimeseries(t, db, timeDelta, devMag, lsets)
	time.Sleep(time.Second*time.Duration(OriginSleep))

	switch typ {
	case 1:
		DBQueryOrigin(t, db, queryLabels, timeDelta)
	case 2:
		DBQueryOriginFirstSample(t, db, queryLabels, timeDelta)
	case 3:
		DBQuerySameHost(t, db, timeDelta)
	case 4:
		DBQuerySameHostFirstSample(t, db, timeDelta)
	default:
		DBQueryOrigin(t, db, queryLabels, timeDelta)
		time.Sleep(time.Second*10)
		DBQueryOriginFirstSample(t, db, queryLabels, timeDelta)
		time.Sleep(time.Second*10)
		DBQuerySameHost(t, db, timeDelta)
		time.Sleep(time.Second*10)
		DBQuerySameHostFirstSample(t, db, timeDelta)
	}
	db.Close()
}

func DBBenchNodeExporterTimeseries(t testing.TB, timeDelta, devMag int64, typ, numSeries, numQueryLabels int) {
	dbPath := "db_bench_ne_timeseries/" + strconv.Itoa(int(timeDelta))
	os.RemoveAll(dbPath)
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	db, err := tsdb_origin.Open(dbPath, logger, nil, &tsdb_origin.Options{
		WALSegmentSize:         -1,
		RetentionDuration:      15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds
		BlockRanges:            ExponentialBlockRanges(int64(2*time.Hour)/1e6, 3, 5),
		NoLockfile:             false,
		AllowOverlappingBlocks: false,
	})
	testutil.Ok(t, err)

	lsets := []origin_labels.Labels{}
	queryLabels := []origin_labels.Label{}

	LoadDevOpsSeries(t, &lsets, &queryLabels, numSeries, numQueryLabels, true)
	if numSeries > 35000 {
		DBAppendCounter(t, db, timeDelta, devMag, 3600*12*1000/int(timeDelta), lsets)
	} else {
		DBAppendNodeExporterTimeseries(t, db, timeDelta, devMag, lsets)
	}
	time.Sleep(time.Second*time.Duration(OriginSleep))

	switch typ {
	case 1:
		DBQueryOrigin(t, db, queryLabels, timeDelta)
	case 2:
		DBQueryOriginFirstSample(t, db, queryLabels, timeDelta)
	case 3:
		DBQuerySameHost(t, db, timeDelta)
	case 4:
		DBQuerySameHostFirstSample(t, db, timeDelta)
	default:
		DBQueryOrigin(t, db, queryLabels, timeDelta)
		time.Sleep(time.Second*10)
		DBQueryOriginFirstSample(t, db, queryLabels, timeDelta)
		time.Sleep(time.Second*10)
		DBQuerySameHost(t, db, timeDelta)
		time.Sleep(time.Second*10)
		DBQuerySameHostFirstSample(t, db, timeDelta)
	}
	db.Close()
}

func GroupDBBenchRandom(t testing.TB, timeDelta, devMag int64, typ, seriesPerGroup, numSeries, scrapeCount, numQueryLabels int) {
	dbPath := "group_db_bench_random/" + strconv.Itoa(int(timeDelta))
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
	testutil.Ok(t, err)

	lsets := []labels.Labels{}
	queryLabels := []labels.Label{}

	GroupLoadDevOpsSeries(t, &lsets, &queryLabels, seriesPerGroup, numSeries, numQueryLabels, true)
	GroupDBAppendRandom(t, db, timeDelta, devMag, seriesPerGroup, scrapeCount, lsets)
	time.Sleep(time.Second*time.Duration(GroupSleep))

	switch typ {
	case 1:
		GroupDBQueryOrigin(t, db, queryLabels, timeDelta)
	case 2:
		GroupDBQueryOriginFirstSample(t, db, queryLabels, timeDelta)
	case 3:
		GroupDBQuerySameHost(t, db, timeDelta)
	case 4:
		GroupDBQuerySameHostFirstSample(t, db, timeDelta)
	default:
		GroupDBQueryOrigin(t, db, queryLabels, timeDelta)
		time.Sleep(time.Second*10)
		GroupDBQueryOriginFirstSample(t, db, queryLabels, timeDelta)
		time.Sleep(time.Second*10)
		GroupDBQuerySameHost(t, db, timeDelta)
		time.Sleep(time.Second*10)
		GroupDBQuerySameHostFirstSample(t, db, timeDelta)
	}
	db.Close()
}

func GroupDBBenchTimeseries(t testing.TB, timeDelta, devMag int64, typ, seriesPerGroup, numSeries, numQueryLabels int) {
	dbPath := "group_db_bench_timeseries/" + strconv.Itoa(int(timeDelta))
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
	testutil.Ok(t, err)

	lsets := []labels.Labels{}
	queryLabels := []labels.Label{}

	GroupLoadDevOpsSeries(t, &lsets, &queryLabels, seriesPerGroup, numSeries, numQueryLabels, true)
	GroupDBAppendTimeseries(t, db, timeDelta, devMag, seriesPerGroup, lsets)
	time.Sleep(time.Second*time.Duration(GroupSleep))

	switch typ {
	case 1:
		GroupDBQueryOrigin(t, db, queryLabels, timeDelta)
	case 2:
		GroupDBQueryOriginFirstSample(t, db, queryLabels, timeDelta)
	case 3:
		GroupDBQuerySameHost(t, db, timeDelta)
	case 4:
		GroupDBQuerySameHostFirstSample(t, db, timeDelta)
	default:
		GroupDBQueryOrigin(t, db, queryLabels, timeDelta)
		time.Sleep(time.Second*10)
		GroupDBQueryOriginFirstSample(t, db, queryLabels, timeDelta)
		time.Sleep(time.Second*10)
		GroupDBQuerySameHost(t, db, timeDelta)
		time.Sleep(time.Second*10)
		GroupDBQuerySameHostFirstSample(t, db, timeDelta)
	}
	db.Close()
}

func GroupDBBenchNodeExporterTimeseries(t testing.TB, timeDelta, devMag int64, typ, seriesPerGroup, numSeries, numQueryLabels int) {
	dbPath := "group_db_bench_ne_timeseries/" + strconv.Itoa(int(timeDelta))
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
	testutil.Ok(t, err)

	lsets := []labels.Labels{}
	queryLabels := []labels.Label{}

	GroupLoadDevOpsSeries(t, &lsets, &queryLabels, seriesPerGroup, numSeries, numQueryLabels, true)
	if numSeries > 35000 {
		GroupDBAppendCounter(t, db, timeDelta, devMag, seriesPerGroup, 3600*12*1000/int(timeDelta), lsets)
	} else {
		GroupDBAppendNodeExporterTimeseries(t, db, timeDelta, devMag, seriesPerGroup, lsets)
	}
	time.Sleep(time.Second*time.Duration(GroupSleep))

	switch typ {
	case 1:
		GroupDBQueryOrigin(t, db, queryLabels, timeDelta)
	case 2:
		GroupDBQueryOriginFirstSample(t, db, queryLabels, timeDelta)
	case 3:
		GroupDBQuerySameHost(t, db, timeDelta)
	case 4:
		GroupDBQuerySameHostFirstSample(t, db, timeDelta)
	default:
		GroupDBQueryOrigin(t, db, queryLabels, timeDelta)
		time.Sleep(time.Second*10)
		GroupDBQueryOriginFirstSample(t, db, queryLabels, timeDelta)
		time.Sleep(time.Second*10)
		GroupDBQuerySameHost(t, db, timeDelta)
		time.Sleep(time.Second*10)
		GroupDBQuerySameHostFirstSample(t, db, timeDelta)
	}
	db.Close()
}

func TestDB(t *testing.T) {
	timeDeltas := []int64{5000, 10000, 15000, 30000}
	for i := 2; i <= 2; i++ {
		for _, timeDelta := range timeDeltas {
			fmt.Println("time delta", timeDelta)
			DBBenchRandom(t, timeDelta, 10, i, 5000, 3600*12*1000/int(timeDelta), 200)
			DBBenchTimeseries(t, timeDelta, 10, i, 5000, 200)
		}
	}
}

func TestGroupDB(t *testing.T) {
	timeDeltas := []int64{5000, 10000, 15000, 30000}
	// timeDeltas := []int64{15000}
	for i := 1; i <= 4; i++ {
		for _, timeDelta := range timeDeltas {
			fmt.Println("time delta", timeDelta)
			GroupDBBenchRandom(t, timeDelta, 10, i, 101, 5000, 3600*12*1000/int(timeDelta), 200)
			GroupDBBenchTimeseries(t, timeDelta, 10, i, 101, 5000, 200)
		}
	}
}

func TestDBComparison(t *testing.T) {
	timeDeltas := []int64{5000, 10000, 15000, 30000}
	// timeDeltas := []int64{15000}
	for i := 1; i <= 1; i++ {
		for _, timeDelta := range timeDeltas {
			fmt.Println("------- time delta", timeDelta)
			DBBenchRandom(t, timeDelta, 100, i, NumSeries, 3600*12*1000/int(timeDelta), 200)
			// // DBBenchTimeseries(t, timeDelta, 100, i, 5000, 200)
			DBBenchNodeExporterTimeseries(t, timeDelta, 100, i, NumSeries, 200)

			GroupDBBenchRandom(t, timeDelta, 100, i, 101, NumSeries, 3600*12*1000/int(timeDelta), 200)
			// GroupDBBenchTimeseries(t, timeDelta, 100, i, 101, 5000, 200)
			GroupDBBenchNodeExporterTimeseries(t, timeDelta, 100, i, 101, NumSeries, 200)
		}
	}
}
