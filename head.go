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

package tsdb

import (
	"fmt"
	"math"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/naivewong/tsdb-group/chunkenc"
	"github.com/naivewong/tsdb-group/encoding"
	"github.com/naivewong/tsdb-group/index"
	"github.com/naivewong/tsdb-group/labels"
	"github.com/naivewong/tsdb-group/wal"
)

var (
	// ErrNotFound is returned if a looked up resource was not found.
	ErrNotFound = errors.Errorf("not found")

	// ErrOutOfOrderSample is returned if an appended sample has a
	// timestamp smaller than the most recent sample.
	ErrOutOfOrderSample = errors.New("out of order sample")

	// ErrAmendSample is returned if an appended sample has the same timestamp
	// as the most recent sample but a different value.
	ErrAmendSample = errors.New("amending sample")

	// ErrOutOfBounds is returned if an appended sample is out of the
	// writable time range.
	ErrOutOfBounds = errors.New("out of bounds")

	// emptyTombstoneReader is a no-op Tombstone Reader.
	// This is used by head to satisfy the Tombstones() function call.
	emptyTombstoneReader = newMemTombstones()
)

// Head handles reads and writes of time series data within a time window.
type Head struct {
	chunkRange int64
	metrics    *headMetrics
	wal        *wal.WAL
	logger     log.Logger
	appendPool sync.Pool
	bytesPool  sync.Pool

	minTime, maxTime int64 // Current min and max of the samples included in the head.
	minValidTime     int64 // Mint allowed to be added to the head. It shouldn't be lower than the maxt of the last persisted block.
	lastSeriesID     uint64
	lastGroupID      uint64

	// All series addressable by their ID or hash.
	series *stripeSeries

	symMtx  sync.RWMutex
	symbols map[string]struct{}
	values  map[string]stringset // label names to possible values

	deletedMtx sync.Mutex
	deleted    map[uint64]int // Deleted series, and what WAL segment they must be kept until.

	postings *index.MemPostings // postings lists for terms
}

type headMetrics struct {
	activeAppenders         prometheus.Gauge
	series                  prometheus.Gauge
	seriesCreated           prometheus.Counter
	seriesRemoved           prometheus.Counter
	seriesNotFound          prometheus.Counter
	// chunks                  prometheus.Gauge
	// chunksCreated           prometheus.Counter
	// chunksRemoved           prometheus.Counter
	gcDuration              prometheus.Summary
	groups                  prometheus.Gauge
	groupsCreated           prometheus.Counter
	groupsRemoved           prometheus.Counter
	minTime                 prometheus.GaugeFunc
	maxTime                 prometheus.GaugeFunc
	samplesAppended         prometheus.Counter
	walTruncateDuration     prometheus.Summary
	walCorruptionsTotal     prometheus.Counter
	headTruncateFail        prometheus.Counter
	headTruncateTotal       prometheus.Counter
	checkpointDeleteFail    prometheus.Counter
	checkpointDeleteTotal   prometheus.Counter
	checkpointCreationFail  prometheus.Counter
	checkpointCreationTotal prometheus.Counter
}

func newHeadMetrics(h *Head, r prometheus.Registerer) *headMetrics {
	m := &headMetrics{}

	m.activeAppenders = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_head_active_appenders",
		Help: "Number of currently active appender transactions",
	})
	m.series = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_head_series",
		Help: "Total number of series in the head block.",
	})
	m.seriesCreated = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_series_created_total",
		Help: "Total number of series created in the head",
	})
	m.seriesRemoved = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_series_removed_total",
		Help: "Total number of series removed in the head",
	})
	m.seriesNotFound = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_series_not_found_total",
		Help: "Total number of requests for series that were not found.",
	})
	// m.chunks = prometheus.NewGauge(prometheus.GaugeOpts{
	// 	Name: "prometheus_tsdb_head_chunks",
	// 	Help: "Total number of chunks in the head block.",
	// })
	// m.chunksCreated = prometheus.NewCounter(prometheus.CounterOpts{
	// 	Name: "prometheus_tsdb_head_chunks_created_total",
	// 	Help: "Total number of chunks created in the head",
	// })
	// m.chunksRemoved = prometheus.NewCounter(prometheus.CounterOpts{
	// 	Name: "prometheus_tsdb_head_chunks_removed_total",
	// 	Help: "Total number of chunks removed in the head",
	// })
	m.gcDuration = prometheus.NewSummary(prometheus.SummaryOpts{
		Name:       "prometheus_tsdb_head_gc_duration_seconds",
		Help:       "Runtime of garbage collection in the head block.",
		Objectives: map[float64]float64{},
	})
	m.groups = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_head_groups",
		Help: "Total number of groups in the head block.",
	})
	m.groupsCreated = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_groups_created_total",
		Help: "Total number of groups created in the head",
	})
	m.groupsRemoved = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_groups_removed_total",
		Help: "Total number of groups removed in the head",
	})
	m.maxTime = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_head_max_time",
		Help: "Maximum timestamp of the head block. The unit is decided by the library consumer.",
	}, func() float64 {
		return float64(h.MaxTime())
	})
	m.minTime = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_head_min_time",
		Help: "Minimum time bound of the head block. The unit is decided by the library consumer.",
	}, func() float64 {
		return float64(h.MinTime())
	})
	m.walTruncateDuration = prometheus.NewSummary(prometheus.SummaryOpts{
		Name:       "prometheus_tsdb_wal_truncate_duration_seconds",
		Help:       "Duration of WAL truncation.",
		Objectives: map[float64]float64{},
	})
	m.walCorruptionsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_wal_corruptions_total",
		Help: "Total number of WAL corruptions.",
	})
	m.samplesAppended = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_samples_appended_total",
		Help: "Total number of appended samples.",
	})
	m.headTruncateFail = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_truncations_failed_total",
		Help: "Total number of head truncations that failed.",
	})
	m.headTruncateTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_truncations_total",
		Help: "Total number of head truncations attempted.",
	})
	m.checkpointDeleteFail = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_checkpoint_deletions_failed_total",
		Help: "Total number of checkpoint deletions that failed.",
	})
	m.checkpointDeleteTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_checkpoint_deletions_total",
		Help: "Total number of checkpoint deletions attempted.",
	})
	m.checkpointCreationFail = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_checkpoint_creations_failed_total",
		Help: "Total number of checkpoint creations that failed.",
	})
	m.checkpointCreationTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_checkpoint_creations_total",
		Help: "Total number of checkpoint creations attempted.",
	})

	if r != nil {
		r.MustRegister(
			m.activeAppenders,
			// m.chunks,
			// m.chunksCreated,
			// m.chunksRemoved,
			m.series,
			m.seriesCreated,
			m.seriesRemoved,
			m.seriesNotFound,
			m.minTime,
			m.maxTime,
			m.gcDuration,
			m.groups,
			m.groupsCreated,
			m.groupsRemoved,
			m.walTruncateDuration,
			m.walCorruptionsTotal,
			m.samplesAppended,
			m.headTruncateFail,
			m.headTruncateTotal,
			m.checkpointDeleteFail,
			m.checkpointDeleteTotal,
			m.checkpointCreationFail,
			m.checkpointCreationTotal,
		)
	}
	return m
}

// NewHead opens the head block in dir.
func NewHead(r prometheus.Registerer, l log.Logger, wal *wal.WAL, chunkRange int64) (*Head, error) {
	if l == nil {
		l = log.NewNopLogger()
	}
	if chunkRange < 1 {
		return nil, errors.Errorf("invalid chunk range %d", chunkRange)
	}
	h := &Head{
		wal:        wal,
		logger:     l,
		chunkRange: chunkRange,
		minTime:    math.MaxInt64,
		maxTime:    math.MinInt64,
		series:     newStripeSeries(),
		values:     map[string]stringset{},
		symbols:    map[string]struct{}{},
		postings:   index.NewUnorderedMemPostings(),
		deleted:    map[uint64]int{},
	}
	h.metrics = newHeadMetrics(h, r)

	return h, nil
}

// processWALSamples adds a partition of samples it receives to the head and passes
// them on to other workers.
// Samples before the mint timestamp are discarded.
func (h *Head) processWALSamples(
	minValidTime int64,
	input <-chan []RefGroupSample, output chan<- []RefGroupSample,
) (unknownRefs uint64) {
	defer close(output)

	// Mitigate lock contention in getByID.
	refSeries := map[uint64]*memSeries{}

	mint, maxt := int64(math.MaxInt64), int64(math.MinInt64)

	for samples := range input {
		for _, s := range samples {
			if s.T < minValidTime {
				continue
			}
			ms := refSeries[s.GroupRef]
			if ms == nil {
				ms = h.series.getGroup(s.GroupRef)
				if ms == nil {
					unknownRefs++
					continue
				}
				refSeries[s.GroupRef] = ms
			}
			ms.appendGroup(s.T, s.Vals)

			if s.T > maxt {
				maxt = s.T
			}
			if s.T < mint {
				mint = s.T
			}
		}
		output <- samples
	}
	h.updateMinMaxTime(mint, maxt)

	return unknownRefs
}

func (h *Head) updateMinMaxTime(mint, maxt int64) {
	for {
		lt := h.MinTime()
		if mint >= lt {
			break
		}
		if atomic.CompareAndSwapInt64(&h.minTime, lt, mint) {
			break
		}
	}
	for {
		ht := h.MaxTime()
		if maxt <= ht {
			break
		}
		if atomic.CompareAndSwapInt64(&h.maxTime, ht, maxt) {
			break
		}
	}
}

func (h *Head) loadWAL(r *wal.Reader, multiRef map[uint64]uint64) (err error) {
	// Track number of samples that referenced a series we don't know about
	// for error reporting.
	var unknownRefs uint64

	// Start workers that each process samples for a partition of the series ID space.
	// They are connected through a ring of channels which ensures that all sample batches
	// read from the WAL are processed in order.
	var (
		wg           sync.WaitGroup
		multiRefLock sync.Mutex
		n            = runtime.GOMAXPROCS(0)
		inputs       = make([]chan []RefGroupSample, n)
		outputs      = make([]chan []RefGroupSample, n)
	)
	wg.Add(n)

	defer func() {
		// For CorruptionErr ensure to terminate all workers before exiting.
		if _, ok := err.(*wal.CorruptionErr); ok {
			for i := 0; i < n; i++ {
				close(inputs[i])
				for range outputs[i] {
				}
			}
			wg.Wait()
		}
	}()

	for i := 0; i < n; i++ {
		outputs[i] = make(chan []RefGroupSample, 300)
		inputs[i] = make(chan []RefGroupSample, 300)

		go func(input <-chan []RefGroupSample, output chan<- []RefGroupSample) {
			unknown := h.processWALSamples(h.minValidTime, input, output)
			atomic.AddUint64(&unknownRefs, unknown)
			wg.Done()
		}(inputs[i], outputs[i])
	}

	var (
		dec       RecordDecoder
		series    []RefGroupSeries
		samples   []RefGroupSample
		tstones   []Stone
		allStones = newMemTombstones()
	)
	defer func() {
		if err := allStones.Close(); err != nil {
			level.Warn(h.logger).Log("msg", "closing  memTombstones during wal read", "err", err)
		}
	}()
	for r.Next() {
		series, samples, tstones = series[:0], samples[:0], tstones[:0]
		rec := r.Record()

		switch dec.Type(rec) {
		case RecordGroupSeries:
			series, err = dec.GroupSeries(rec, series)
			if err != nil {
				return &wal.CorruptionErr{
					Err:     errors.Wrap(err, "decode group series"),
					Segment: r.Segment(),
					Offset:  r.Offset(),
				}
			}

			for _, rgs := range series {
				var labels []labels.Labels
				var ids    []uint64
				for _, s := range rgs.Series {
					labels = append(labels, s.Labels)
					ids = append(ids, s.Ref)
					if h.lastSeriesID < s.Ref {
						h.lastSeriesID = s.Ref
					}
				}
				ms, created := h.getOrCreateWithID(labels, rgs.GroupRef, ids)

				if !created {
					// There's already a different ref for this series.
					multiRefLock.Lock()
					multiRef[rgs.GroupRef] = ms.groupRef
					multiRefLock.Unlock()
				}

				if h.lastGroupID < rgs.GroupRef {
					h.lastGroupID = rgs.GroupRef
				}
			}
		case RecordGroupSamples:
			samples, err = dec.GroupSamples(rec, samples)
			s := samples
			if err != nil {
				return &wal.CorruptionErr{
					Err:     errors.Wrap(err, "decode group samples"),
					Segment: r.Segment(),
					Offset:  r.Offset(),
				}
			}
			// We split up the samples into chunks of 500 samples or less.
			// With O(300 * #cores) in-flight sample batches, large scrapes could otherwise
			// cause thousands of very large in flight buffers occupying large amounts
			// of unused memory.
			for len(samples) > 0 {
				m := 500
				if len(samples) < m {
					m = len(samples)
				}
				shards := make([][]RefGroupSample, n)
				for i := 0; i < n; i++ {
					var buf []RefGroupSample
					select {
					case buf = <-outputs[i]:
					default:
					}
					shards[i] = buf[:0]
				}
				for _, sam := range samples[:m] {
					if r, ok := multiRef[sam.GroupRef]; ok {
						sam.GroupRef = r
					}
					mod := sam.GroupRef % uint64(n)
					shards[mod] = append(shards[mod], sam)
				}
				for i := 0; i < n; i++ {
					inputs[i] <- shards[i]
				}
				samples = samples[m:]
			}
			samples = s // Keep whole slice for reuse.
		case RecordGroupTombstones:
			tstones, err = dec.GroupTombstones(rec, tstones)
			if err != nil {
				return &wal.CorruptionErr{
					Err:     errors.Wrap(err, "decode group tombstones"),
					Segment: r.Segment(),
					Offset:  r.Offset(),
				}
			}
			for _, s := range tstones {
				for _, itv := range s.intervals {
					if itv.Maxt < h.minValidTime {
						continue
					}
					if m := h.series.getByID(s.ref); m == nil {
						unknownRefs++
						continue
					}
					allStones.addInterval(s.ref, itv)
				}
			}
		default:
			return &wal.CorruptionErr{
				Err:     errors.Errorf("invalid record type %v", dec.Type(rec)),
				Segment: r.Segment(),
				Offset:  r.Offset(),
			}
		}
	}

	// Signal termination to each worker and wait for it to close its output channel.
	for i := 0; i < n; i++ {
		close(inputs[i])
		for range outputs[i] {
		}
	}
	wg.Wait()

	if r.Err() != nil {
		return errors.Wrap(r.Err(), "read records")
	}

	if err := allStones.Iter(func(ref uint64, dranges Intervals) error {
		return h.chunkRewrite(ref, dranges, true)
	}); err != nil {
		return errors.Wrap(r.Err(), "deleting samples from tombstones")
	}

	if unknownRefs > 0 {
		level.Warn(h.logger).Log("msg", "unknown series references", "count", unknownRefs)
	}
	return nil
}

// Init loads data from the write ahead log and prepares the head for writes.
// It should be called before using an appender so that
// limits the ingested samples to the head min valid time.
func (h *Head) Init(minValidTime int64) error {
	h.minValidTime = minValidTime
	defer h.postings.EnsureOrder()
	defer h.gc() // After loading the wal remove the obsolete data from the head.

	if h.wal == nil {
		return nil
	}

	// Backfill the checkpoint first if it exists.
	dir, startFrom, err := LastCheckpoint(h.wal.Dir())
	if err != nil && err != ErrNotFound {
		return errors.Wrap(err, "find last checkpoint")
	}
	multiRef := map[uint64]uint64{}
	if err == nil {
		sr, err := wal.NewSegmentsReader(dir)
		if err != nil {
			return errors.Wrap(err, "open checkpoint")
		}
		defer func() {
			if err := sr.Close(); err != nil {
				level.Warn(h.logger).Log("msg", "error while closing the wal segments reader", "err", err)
			}
		}()

		// A corrupted checkpoint is a hard error for now and requires user
		// intervention. There's likely little data that can be recovered anyway.
		if err := h.loadWAL(wal.NewReader(sr), multiRef); err != nil {
			return errors.Wrap(err, "backfill checkpoint")
		}
		startFrom++
	}

	// Find the last segment.
	_, last, err := h.wal.Segments()
	if err != nil {
		return errors.Wrap(err, "finding WAL segments")
	}

	// Backfill segments from the most recent checkpoint onwards.
	for i := startFrom; i <= last; i++ {
		s, err := wal.OpenReadSegment(wal.SegmentName(h.wal.Dir(), i))
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("open WAL segment: %d", i))
		}

		sr := wal.NewSegmentBufReader(s)
		err = h.loadWAL(wal.NewReader(sr), multiRef)
		if err := sr.Close(); err != nil {
			level.Warn(h.logger).Log("msg", "error while closing the wal segments reader", "err", err)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

// Truncate removes old data before mint from the head.
func (h *Head) Truncate(mint int64) (err error) {
	defer func() {
		if err != nil {
			h.metrics.headTruncateFail.Inc()
		}
	}()
	initialize := h.MinTime() == math.MaxInt64

	if h.MinTime() >= mint && !initialize {
		return nil
	}
	atomic.StoreInt64(&h.minTime, mint)
	atomic.StoreInt64(&h.minValidTime, mint)

	// Ensure that max time is at least as high as min time.
	for h.MaxTime() < mint {
		atomic.CompareAndSwapInt64(&h.maxTime, h.MaxTime(), mint)
	}

	// This was an initial call to Truncate after loading blocks on startup.
	// We haven't read back the WAL yet, so do not attempt to truncate it.
	if initialize {
		return nil
	}

	h.metrics.headTruncateTotal.Inc()
	start := time.Now()

	h.gc()
	level.Info(h.logger).Log("msg", "head GC completed", "duration", time.Since(start))
	h.metrics.gcDuration.Observe(time.Since(start).Seconds())

	if h.wal == nil {
		return nil
	}
	start = time.Now()

	first, last, err := h.wal.Segments()
	if err != nil {
		return errors.Wrap(err, "get segment range")
	}
	// Start a new segment, so low ingestion volume TSDB don't have more WAL than
	// needed.
	err = h.wal.NextSegment()
	if err != nil {
		return errors.Wrap(err, "next segment")
	}
	last-- // Never consider last segment for checkpoint.
	if last < 0 {
		return nil // no segments yet.
	}
	// The lower third of segments should contain mostly obsolete samples.
	// If we have less than three segments, it's not worth checkpointing yet.
	last = first + (last-first)/3
	if last <= first {
		return nil
	}

	keep := func(id uint64) bool {
		if h.series.getByID(id) != nil {
			return true
		}
		h.deletedMtx.Lock()
		_, ok := h.deleted[id]
		h.deletedMtx.Unlock()
		return ok
	}
	h.metrics.checkpointCreationTotal.Inc()
	if _, err = Checkpoint(h.wal, first, last, keep, mint); err != nil {
		h.metrics.checkpointCreationFail.Inc()
		return errors.Wrap(err, "create checkpoint")
	}
	if err := h.wal.Truncate(last + 1); err != nil {
		// If truncating fails, we'll just try again at the next checkpoint.
		// Leftover segments will just be ignored in the future if there's a checkpoint
		// that supersedes them.
		level.Error(h.logger).Log("msg", "truncating segments failed", "err", err)
	}

	// The checkpoint is written and segments before it is truncated, so we no
	// longer need to track deleted series that are before it.
	h.deletedMtx.Lock()
	for ref, segment := range h.deleted {
		if segment < first {
			delete(h.deleted, ref)
		}
	}
	h.deletedMtx.Unlock()

	h.metrics.checkpointDeleteTotal.Inc()
	if err := DeleteCheckpoints(h.wal.Dir(), last); err != nil {
		// Leftover old checkpoints do not cause problems down the line beyond
		// occupying disk space.
		// They will just be ignored since a higher checkpoint exists.
		level.Error(h.logger).Log("msg", "delete old checkpoints", "err", err)
		h.metrics.checkpointDeleteFail.Inc()
	}
	h.metrics.walTruncateDuration.Observe(time.Since(start).Seconds())

	level.Info(h.logger).Log("msg", "WAL checkpoint complete",
		"first", first, "last", last, "duration", time.Since(start))

	return nil
}

// initTime initializes a head with the first timestamp. This only needs to be called
// for a completely fresh head with an empty WAL.
// Returns true if the initialization took an effect.
func (h *Head) initTime(t int64) (initialized bool) {
	if !atomic.CompareAndSwapInt64(&h.minTime, math.MaxInt64, t) {
		return false
	}
	// Ensure that max time is initialized to at least the min time we just set.
	// Concurrent appenders may already have set it to a higher value.
	atomic.CompareAndSwapInt64(&h.maxTime, math.MinInt64, t)

	return true
}

type rangeHead struct {
	head       *Head
	mint, maxt int64
}

func (h *rangeHead) Index() (IndexReader, error) {
	return h.head.indexRange(h.mint, h.maxt), nil
}

func (h *rangeHead) Chunks() (ChunkReader, error) {
	return h.head.chunksRange(h.mint, h.maxt), nil
}

func (h *rangeHead) Tombstones() (TombstoneReader, error) {
	return emptyTombstoneReader, nil
}

func (h *rangeHead) MinTime() int64 {
	return h.mint
}

func (h *rangeHead) MaxTime() int64 {
	return h.maxt
}

// initAppender is a helper to initialize the time bounds of the head
// upon the first sample it receives.
type initAppender struct {
	app  Appender
	head *Head
}

func (a *initAppender) Add(lset labels.Labels, t int64, v float64) (uint64, error) {
	return 0, errors.Errorf("disabled in group head")
}

func (a *initAppender) AddFast(ref uint64, t int64, v float64) error {
	return errors.Errorf("disabled in group head")
}

func (a *initAppender) AddGroup(l []labels.Labels, t int64, v []float64) (uint64, error) {
	if a.app != nil {
		return a.app.AddGroup(l, t, v)
	}
	a.head.initTime(t)
	a.app = a.head.appender()

	return a.app.AddGroup(l, t, v)
}

func (a *initAppender) AddGroupFast(ref uint64, t int64, v []float64) error {
	if a.app == nil {
		return ErrNotFound
	}
	return a.app.AddGroupFast(ref, t, v)
}

func (a *initAppender) Commit() error {
	if a.app == nil {
		return nil
	}
	return a.app.Commit()
}

func (a *initAppender) Rollback() error {
	if a.app == nil {
		return nil
	}
	return a.app.Rollback()
}

// Appender returns a new Appender on the database.
func (h *Head) Appender() Appender {
	h.metrics.activeAppenders.Inc()

	// The head cache might not have a starting point yet. The init appender
	// picks up the first appended timestamp as the base.
	if h.MinTime() == math.MaxInt64 {
		return &initAppender{head: h}
	}
	return h.appender()
}

func (h *Head) appender() *headAppender {
	return &headAppender{
		head: h,
		// Set the minimum valid time to whichever is greater the head min valid time or the compaciton window.
		// This ensures that no samples will be added within the compaction window to avoid races.
		minValidTime: max(atomic.LoadInt64(&h.minValidTime), h.MaxTime()-h.chunkRange/2),
		mint:         math.MaxInt64,
		maxt:         math.MinInt64,
		samples:      h.getAppendBuffer(),
	}
}

func (h *Head) getAppendBuffer() []RefGroupSample {
	b := h.appendPool.Get()
	if b == nil {
		return make([]RefGroupSample, 0, 512)
	}
	return b.([]RefGroupSample)
}

func (h *Head) putAppendBuffer(b []RefGroupSample) {
	//lint:ignore SA6002 safe to ignore and actually fixing it has some performance penalty.
	h.appendPool.Put(b[:0])
}

func (h *Head) getBytesBuffer() []byte {
	b := h.bytesPool.Get()
	if b == nil {
		return make([]byte, 0, 1024)
	}
	return b.([]byte)
}

func (h *Head) putBytesBuffer(b []byte) {
	//lint:ignore SA6002 safe to ignore and actually fixing it has some performance penalty.
	h.bytesPool.Put(b[:0])
}

type headAppender struct {
	head         *Head
	minValidTime int64 // No samples below this timestamp are allowed.
	mint, maxt   int64

	series  []RefGroupSeries
	samples []RefGroupSample
}

func (a *headAppender) Add(lset labels.Labels, t int64, v float64) (uint64, error) {
	return 0, errors.Errorf("disabled in group head")
}

func (a *headAppender) AddFast(ref uint64, t int64, v float64) error {
	return errors.Errorf("disabled in group head")
}

func (a *headAppender) AddGroup(l []labels.Labels, t int64, v []float64) (uint64, error) {
	if t < a.minValidTime {
		return 0, ErrOutOfBounds
	}

	s, created := a.head.getOrCreate(l)
	if created {
		rseries := make([]RefSeries, 0, len(s.ids))
		for i := 0; i < len(s.ids); i++ {
			rseries = append(rseries, RefSeries{Ref: s.ids[i], Labels: s.labels[i]})
		}
		a.series = append(a.series, RefGroupSeries{
			GroupRef:    s.groupRef,
			Series:      rseries,
		})
	}
	return s.groupRef, a.AddGroupFast(s.groupRef, t, v)
}

func (a *headAppender) AddGroupFast(ref uint64, t int64, v []float64) error {
	if t < a.minValidTime {
		return ErrOutOfBounds
	}

	s := a.head.series.getGroup(ref)
	if s == nil {
		return errors.Wrap(ErrNotFound, "unknown group")
	}

	// TODO(Alec), figure out if lock here.
        // It's meaningless to have multiple appenders on the same Memseries in current
        // use case because it can trigger unneccessary ErrOutOfOrderSample easily.
	s.Lock()
	if err := s.appendable(t); err != nil {
		s.Unlock()
		return err
	}
	s.pendingCommit = true
	s.Unlock()

	// NOTE(Alec), move this to commit is more reasonable.
	// if t < a.mint {
	// 	a.mint = t
	// }
	// if t > a.maxt {
	// 	a.maxt = t
	// }

	rsample := RefGroupSample{
		GroupRef: ref,
		T:        t,
		Ids:      make([]uint64, 0, len(v)),
		Vals:     make([]float64, 0, len(v)),
		series:   s,
	}
	for i := 0; i < len(v); i++ {
		rsample.Ids = append(rsample.Ids, s.ids[i])
		rsample.Vals = append(rsample.Vals, v[i])
	}
	a.samples = append(a.samples, rsample)
	return nil
}

func (a *headAppender) log() error {
	if a.head.wal == nil {
		return nil
	}

	buf := a.head.getBytesBuffer()
	defer func() { a.head.putBytesBuffer(buf) }()

	var rec []byte
	var enc RecordEncoder

	if len(a.series) > 0 {
		rec = enc.GroupSeries(a.series, buf)
		buf = rec[:0]

		if err := a.head.wal.Log(rec); err != nil {
			return errors.Wrap(err, "log group series")
		}
	}
	if len(a.samples) > 0 {
		rec = enc.GroupSamples(a.samples, buf)
		buf = rec[:0]

		if err := a.head.wal.Log(rec); err != nil {
			return errors.Wrap(err, "log group samples")
		}
	}
	return nil
}

func (a *headAppender) Commit() error {
	defer a.head.metrics.activeAppenders.Dec()
	defer a.head.putAppendBuffer(a.samples)

	if err := a.log(); err != nil {
		return errors.Wrap(err, "write to WAL")
	}

	total := 0

	for _, s := range a.samples {
		s.series.Lock()
		ok := s.series.appendGroup(s.T, s.Vals)
		s.series.pendingCommit = false
		s.series.Unlock()

		if ok {
			total += len(s.Vals)
			if s.T < a.mint {
				a.mint = s.T
			}
			if s.T > a.maxt {
				a.maxt = s.T
			}
		}
	}

	// TODO(Alec), the original tsdb doesn't have this.
	a.series = a.series[:0]
	a.samples = a.samples[:0]

	a.head.metrics.samplesAppended.Add(float64(total))
	a.head.updateMinMaxTime(a.mint, a.maxt)

	return nil
}

func (a *headAppender) Rollback() error {
	a.head.metrics.activeAppenders.Dec()
	for _, s := range a.samples {
		s.series.Lock()
		s.series.pendingCommit = false
		s.series.Unlock()
	}
	a.head.putAppendBuffer(a.samples)

	// Series are created in the head memory regardless of rollback. Thus we have
	// to log them to the WAL in any case.
	a.samples = nil
	return a.log()
}

// Delete all samples in the range of [mint, maxt] for series that satisfy the given
// label matchers.
func (h *Head) Delete(mint, maxt int64, ms ...labels.Matcher) error {
	// Do not delete anything beyond the currently valid range.
	mint, maxt = clampInterval(mint, maxt, h.MinTime(), h.MaxTime())

	if mint > maxt {
		// return errors.Errorf("delete range outside the GroupHead range")
		// Return because of out-of-bounds.
		return nil
	}

	ir := h.indexRange(mint, maxt)

	p, err := PostingsForMatchers(ir, ms...)
	if err != nil {
		return errors.Wrap(err, "select series")
	}

	var stones []Stone
	dirty := false
	itvls := make(map[uint64]Intervals)
	for p.Next() {
		series := h.series.getByID(p.At())

		// Deduplicate the repeated groups.
		if _, ok := itvls[series.groupRef]; ok {
			continue
		}

		t0, t1 := series.minTime(), series.maxTime()
		if t0 == math.MinInt64 || t1 == math.MinInt64 {
			continue
		}
		// Delete only until the current values and not beyond.
		t0, t1 = clampInterval(mint, maxt, t0, t1)
		if t0 > t1 {
			continue
		}

		itvls[series.groupRef] = Intervals{{t0, t1}}

		if h.wal != nil {
			stones = append(stones, Stone{p.At(), Intervals{{t0, t1}}})
		}
	}
	if p.Err() != nil {
		return p.Err()
	}
	for gid, dranges := range itvls {
		if err := h.chunkRewrite(gid, dranges, true); err != nil {
			return errors.Wrap(err, "delete samples")
		}
		dirty = true
	}
	var enc RecordEncoder
	if h.wal != nil {
		// Although we don't store the stones in the head
		// we need to write them to the WAL to mark these as deleted
		// after a restart while loading the WAL.
		if err := h.wal.Log(enc.GroupTombstones(stones, nil)); err != nil {
			return err
		}
	}
	if dirty {
		h.gc()
	}

	return nil
}

// if group=true, ref is group id.
// else, ref is series id.
func (h *Head) chunkRewrite(ref uint64, dranges Intervals, group bool) (err error) {
	if len(dranges) == 0 {
		return nil
	}
	var ms *memSeries
	if group {
		ms = h.series.getGroup(ref)
	} else {
		ms = h.series.getByID(ref)
	}
	if ms == nil {
		return nil
	}
	ms.Lock()
	defer ms.Unlock()

	iterators := make([]*chunkSeriesIterator, 0, len(ms.labels))
	for i := 0; i < len(ms.labels); i++ {
		iterators = append(iterators, newChunkSeriesIterator(
			[]chunkenc.Meta{chunkenc.Meta{Ref: ms.groupRef, SeriesRef: uint64(i), Chunk: ms.groupChunk, MinTime: ms.minTime(), MaxTime: ms.maxTime()}},
			dranges,
			ms.minTime(),
			ms.maxTime(),
		))
	}

	ms.reset()
	values := make([]float64, 0, len(iterators))
	for iterators[0].Next() {
		values = values[:0]

		t, v := iterators[0].At()
		values = append(values, v)
		for i := 1; i < len(iterators); i++ {
			iterators[i].Next()
			_, v := iterators[i].At()
			values = append(values, v)
		}
		if !ms.appendGroup(t, values) {
			level.Warn(h.logger).Log("msg", "failed to add sample during delete")
		}
	}

	return nil
}

// gc removes data before the minimum timestamp from the head.
func (h *Head) gc() {
	// Only data strictly lower than this timestamp must be deleted.
	mint := h.MinTime()

	// Drop old chunks and remember series IDs and hashes if they can be
	// deleted entirely.
	deleted, groupsRemoved := h.series.gc(mint)
	seriesRemoved := len(deleted)

	h.metrics.seriesRemoved.Add(float64(seriesRemoved))
	h.metrics.series.Sub(float64(seriesRemoved))
	h.metrics.groupsRemoved.Add(float64(groupsRemoved))
	h.metrics.groups.Sub(float64(groupsRemoved))

	// Remove deleted series IDs from the postings lists.
	h.postings.Delete(deleted)

	if h.wal != nil {
		_, last, _ := h.wal.Segments()
		h.deletedMtx.Lock()
		// Keep series records until we're past segment 'last'
		// because the WAL will still have samples records with
		// this ref ID. If we didn't keep these series records then
		// on start up when we replay the WAL, or any other code
		// that reads the WAL, wouldn't be able to use those
		// samples since we would have no labels for that ref ID.
		for ref := range deleted {
			h.deleted[ref] = last
		}
		h.deletedMtx.Unlock()
	}

	// Rebuild symbols and label value indices from what is left in the postings terms.
	symbols := make(map[string]struct{}, len(h.symbols))
	values := make(map[string]stringset, len(h.values))

	if err := h.postings.Iter(func(t labels.Label, _ index.Postings) error {
		symbols[t.Name] = struct{}{}
		symbols[t.Value] = struct{}{}

		ss, ok := values[t.Name]
		if !ok {
			ss = stringset{}
			values[t.Name] = ss
		}
		ss.set(t.Value)
		return nil
	}); err != nil {
		// This should never happen, as the iteration function only returns nil.
		panic(err)
	}

	h.symMtx.Lock()

	h.symbols = symbols
	h.values = values

	h.symMtx.Unlock()
}

// Tombstones returns a new reader over the head's tombstones
func (h *Head) Tombstones() (TombstoneReader, error) {
	return emptyTombstoneReader, nil
}

// Index returns an IndexReader against the block.
func (h *Head) Index() (IndexReader, error) {
	return h.indexRange(math.MinInt64, math.MaxInt64), nil
}

func (h *Head) indexRange(mint, maxt int64) *headIndexReader {
	if hmin := h.MinTime(); hmin > mint {
		mint = hmin
	}
	return &headIndexReader{head: h, mint: mint, maxt: maxt}
}

// Chunks returns a ChunkReader against the block.
func (h *Head) Chunks() (ChunkReader, error) {
	return h.chunksRange(math.MinInt64, math.MaxInt64), nil
}

func (h *Head) chunksRange(mint, maxt int64) *headChunkReader {
	if hmin := h.MinTime(); hmin > mint {
		mint = hmin
	}
	return &headChunkReader{head: h, mint: mint, maxt: maxt}
}

// MinTime returns the lowest time bound on visible data in the head.
func (h *Head) MinTime() int64 {
	return atomic.LoadInt64(&h.minTime)
}

// MaxTime returns the highest timestamp seen in data of the head.
func (h *Head) MaxTime() int64 {
	return atomic.LoadInt64(&h.maxTime)
}

// compactable returns whether the head has a compactable range.
// The head has a compactable range when the head time range is 1.5 times the chunk range.
// The 0.5 acts as a buffer of the appendable window.
func (h *Head) compactable() bool {
	return h.MaxTime()-h.MinTime() > h.chunkRange/2*3
}

func (h *Head) Size() int {
	h.series.gLock.RLock()
	defer h.series.gLock.RUnlock()
	total := 0
	for _, g := range h.series.groups {
		total += g.groupChunk.Size()
	}
	return total
}

func (h *Head) NumGroups() int {
	h.series.gLock.RLock()
	defer h.series.gLock.RUnlock()
	return len(h.series.groups)
}

func (h *Head) NumSamples() int {
	h.series.gLock.RLock()
	defer h.series.gLock.RUnlock()
	total := 0
	for _, g := range h.series.groups {
		total += g.groupChunk.NumSamples()
	}
	return total
}

// Close flushes the WAL and closes the head.
func (h *Head) Close() error {
	if h.wal == nil {
		return nil
	}
	return h.wal.Close()
}

// packChunkID packs a seriesID and a chunkID within it into a global 8 byte ID.
// It panicks if the seriesID exceeds 5 bytes or the chunk ID 3 bytes.
func packChunkID(seriesID, chunkID uint64) uint64 {
	if seriesID > (1<<40)-1 {
		panic("series ID exceeds 5 bytes")
	}
	if chunkID > (1<<24)-1 {
		panic("chunk ID exceeds 3 bytes")
	}
	return (seriesID << 24) | chunkID
}

func unpackChunkID(id uint64) (seriesID, chunkID uint64) {
	return id >> 24, (id << 40) >> 40
}

type headChunkReader struct {
	head       *Head
	mint, maxt int64
}

func (h *headChunkReader) Close() error {
	return nil
}

// NOTE(Alec), ref here is group ref.
func (h *headChunkReader) Chunk(ref uint64) (chunkenc.Chunk, error) {
	s := h.head.series.getGroup(ref)
	// This means that the series has been garbage collected.
	if s == nil {
		return nil, ErrNotFound
	}

	s.Lock()
	// This means that the chunk has been garbage collected or is outside
	// the specified range.
	if s.groupChunk == nil || !(h.mint <= s.maxTime() && h.maxt >= s.minTime()) {
		s.Unlock()
		return nil, ErrNotFound
	}
	c := &SafeChunk{
		C: s.groupChunk,
		s: s,
	}
	s.Unlock()

	return c, nil
}

type SafeChunk struct {
	C *chunkenc.GroupMemoryChunk1
	s *memSeries
}
func (c *SafeChunk) Bytes() []byte { return c.C.Bytes() }
func (c *SafeChunk) Encoding() chunkenc.Encoding {return chunkenc.EncGHC1 }
func (c *SafeChunk) NumSamples() int { return c.C.NumSamples() }

func (c *SafeChunk) Appender() (chunkenc.Appender, error) {
	return chunkenc.NewNopAppender(), nil
}

func (c *SafeChunk) Iterator() chunkenc.Iterator {
	return chunkenc.NewNopIterator()
}

// n here is the series ref, not the idx inside group.
func (c *SafeChunk) IteratorGroup(t int64, n int) chunkenc.Iterator {
	c.s.Lock()
	it := c.C.IteratorGroup(t, c.s.refIndices[uint64(n)])
	c.s.Unlock()
	return it
}

func (c *SafeChunk) Chunk() chunkenc.Chunk { return c.C }

func min(a int64, b int64) int64 {
	if a <= b {
		return a
	} else {
		return b
	}
}

func max(a int64, b int64) int64 {
	if a >= b {
		return a
	} else {
		return b
	}
}

type headIndexReader struct {
	head       *Head
	mint, maxt int64
}

func (h *headIndexReader) Close() error {
	return nil
}

func (h *headIndexReader) Symbols() (map[string]struct{}, error) {
	h.head.symMtx.RLock()
	defer h.head.symMtx.RUnlock()

	res := make(map[string]struct{}, len(h.head.symbols))

	for s := range h.head.symbols {
		res[s] = struct{}{}
	}
	return res, nil
}

// LabelValues returns the possible label values
func (h *headIndexReader) LabelValues(names ...string) (index.StringTuples, error) {
	if len(names) != 1 {
		return nil, encoding.ErrInvalidSize
	}

	h.head.symMtx.RLock()
	sl := make([]string, 0, len(h.head.values[names[0]]))
	for s := range h.head.values[names[0]] {
		sl = append(sl, s)
	}
	h.head.symMtx.RUnlock()
	sort.Strings(sl)

	return index.NewStringTuples(sl, len(names))
}

// LabelNames returns all the unique label names present in the head.
func (h *headIndexReader) LabelNames() ([]string, error) {
	h.head.symMtx.RLock()
	defer h.head.symMtx.RUnlock()
	labelNames := make([]string, 0, len(h.head.values))
	for name := range h.head.values {
		if name == "" {
			continue
		}
		labelNames = append(labelNames, name)
	}
	sort.Strings(labelNames)
	return labelNames, nil
}

// Postings returns the postings list iterator for the label pair.
func (h *headIndexReader) Postings(name, value string) (index.Postings, error) {
	return h.head.postings.Get(name, value), nil
}

func (h *headIndexReader) GroupPostings(ref uint64) (index.Postings, error) {
	// Return the sorted group ref.(sorted by the first series' labels)
	if ref == index.AllGroupPostings {
		return index.NewListPostings(h.head.series.getSortedGroup()), nil
	}
	s := h.head.series.getGroup(ref)
	if s == nil {
		return index.EmptyPostings(), ErrNotFound
	}
	return index.NewListPostings(s.ids), nil
}

func (h *headIndexReader) SortedPostings(p index.Postings) index.Postings {
	series := make([]*RefSeries, 0, 128)

	// Fetch all the series only once.
	for p.Next() {
		s := h.head.series.getByID(p.At())
		if s == nil {
			level.Debug(h.head.logger).Log("msg", "looked up series not found")
		} else {
			series = append(series, &RefSeries{Ref: p.At(), Labels: s.labels[s.refIndices[p.At()]]})
		}
	}
	if err := p.Err(); err != nil {
		return index.ErrPostings(errors.Wrap(err, "expand postings"))
	}

	sort.Slice(series, func(i, j int) bool {
		return labels.Compare(series[i].Labels, series[j].Labels) < 0
	})

	// Convert back to list.
	ep := make([]uint64, 0, len(series))
	for _, p := range series {
		ep = append(ep, p.Ref)
	}
	return index.NewListPostings(ep)
}

// NOTE(Alec), this is not to sort the content inside a group postings entry but sort all the groups by their first series' labels.
func (h *headIndexReader) SortedGroupPostings(p index.Postings) index.Postings {
	series := make([]*RefSeries, 0, 128)

	for p.Next() {
		s := h.head.series.getGroup(p.At())
		if s == nil {
			level.Debug(h.head.logger).Log("msg", "looked up group not found")
		} else {
			series = append(series, &RefSeries{Ref: p.At(), Labels: s.labels[0]})
		}
	}
	sort.Slice(series, func(i, j int) bool {
		return labels.Compare(series[i].Labels, series[j].Labels) < 0
	})

	// Convert back to list.
	ep := make([]uint64, 0, len(series))
	for _, p := range series {
		ep = append(ep, p.Ref)
	}
	return index.NewListPostings(ep)
}

// Series returns the series for the given reference.
func (h *headIndexReader) Series(ref uint64, lbls *labels.Labels, chks *[]chunkenc.Meta) error {
	s := h.head.series.getByID(ref)

	if s == nil {
		h.head.metrics.seriesNotFound.Inc()
		return ErrNotFound
	}
	*lbls = append((*lbls)[:0], s.labels[s.refIndices[ref]]...)

	s.Lock()
	defer s.Unlock()

	*chks = (*chks)[:0]

	if h.mint > s.maxTime() || h.maxt < s.minTime() {
		// TODO(Alec), to decide whether return error or not.
		// return ErrOutOfBounds
		return nil
	}

	*chks = append(*chks, chunkenc.Meta{
		Ref:             s.groupRef,
		SeriesRef:       ref,
		LogicalGroupRef: s.groupRef,   // NOTE(Alec), can be used in Delete in block.go.
		Chunk:           s.groupChunk, // NOTE(Alec), currently not neccessary because later it will be replaced by SafeChunk.
		MinTime:         max(s.minTime(), h.mint),
		MaxTime:         min(s.maxTime(), h.maxt),
	})

	return nil
}

// NOTE: This is deprecated. Use `LabelNames()` instead.
func (h *headIndexReader) LabelIndices() ([][]string, error) {
	h.head.symMtx.RLock()
	defer h.head.symMtx.RUnlock()
	res := [][]string{}
	for s := range h.head.values {
		res = append(res, []string{s})
	}
	return res, nil
}

func (h *Head) getOrCreate(lset []labels.Labels) (*memSeries, bool) {
	// Note(Alec), here I only check if the first labels exists to determine whether the group exists.
	s := h.series.getByHash(lset[0].Hash(), lset[0])
	if s != nil {
		return s, false
	}

	// Optimistically assume that we are the first one to create the series.
	groupRef := atomic.AddUint64(&h.lastGroupID, 1)
	seriesIds := make([]uint64, 0, len(lset))
	for i := 0; i < len(lset); i++ {
		seriesIds = append(seriesIds, atomic.AddUint64(&h.lastSeriesID, 1))
	}

	return h.getOrCreateWithID(lset, groupRef, seriesIds)
}

func (h *Head) getOrCreateWithID(lset []labels.Labels, groupRef uint64, ids []uint64) (*memSeries, bool) {
	s2 := h.series.getByHash(lset[0].Hash(), lset[0])
	if s2 != nil {
		return s2, false
	}

	s2 = h.series.addGroup(lset, groupRef, ids)
	for i := 0; i < len(lset); i++ {
		h.postings.Add(ids[i], lset[i])
	}

	h.metrics.groups.Inc()
	h.metrics.groupsCreated.Inc()
	h.metrics.series.Add(float64(len(lset)))
	h.metrics.seriesCreated.Add(float64(len(lset)))

	h.symMtx.Lock()
	defer h.symMtx.Unlock()

	for _, lbs := range lset {
		for _, l := range lbs {
			valset, ok := h.values[l.Name]
			if !ok {
				valset = stringset{}
				h.values[l.Name] = valset
			}
			valset.set(l.Value)

			h.symbols[l.Name] = struct{}{}
			h.symbols[l.Value] = struct{}{}
		}
	}
	return s2, true
}

// seriesHashmap is a simple hashmap for memSeries by their label set. It is built
// on top of a regular hashmap and holds a slice of series to resolve hash collisions.
// Its methods require the hash to be submitted with it to avoid re-computations throughout
// the code.
type seriesHashmap map[uint64][]*memSeries

func (m seriesHashmap) get(hash uint64, lset labels.Labels) *memSeries {
	for _, s := range m[hash] {
		for _, ls := range s.labels {
			if ls.Equals(lset) {
				return s
			}
		}
	}
	return nil
}

func (m seriesHashmap) set(hash uint64, s *memSeries) {
	l := m[hash]
	for i, prev := range l {
		if prev.groupRef == s.groupRef {
			l[i] = s
			return
		}
	}
	m[hash] = append(l, s)
}

func (m seriesHashmap) delByLabels(hash uint64, lset labels.Labels) {
	var rem []*memSeries
	for _, s := range m[hash] {
		found := false
		for _, ls := range s.labels {
			// Check if the lset is inside s.labels.
			if ls.Equals(lset) {
				found = true
				break
			}
		}
		if !found {
			rem = append(rem, s)
		}
	}
	if len(rem) == 0 {
		delete(m, hash)
	} else {
		m[hash] = rem
	}
}

func (m seriesHashmap) delByRef(hash uint64, ref uint64) {
	var rem []*memSeries
	for _, s := range m[hash] {
		// Check if the ref is in the refIndices.
		if _, ok := s.refIndices[ref]; !ok {
			rem = append(rem, s)
		}
	}
	if len(rem) == 0 {
		delete(m, hash)
	} else {
		m[hash] = rem
	}
}

func (m seriesHashmap) delByHash(hash uint64) {
	delete(m, hash)
}

// stripeSeries locks modulo ranges of IDs and hashes to reduce lock contention.
// The locks are padded to not be on the same cache line. Filling the padded space
// with the maps was profiled to be slower – likely due to the additional pointer
// dereferences.
type stripeSeries struct {
	series [stripeSize]map[uint64]*memSeries
	hashes [stripeSize]seriesHashmap
	locks  [stripeSize]stripeLock

	groups map[uint64]*memSeries // Map the group ref to memSeries.
	gLock  sync.RWMutex
}

const (
	stripeSize = 1 << 14
	stripeMask = stripeSize - 1
)

type stripeLock struct {
	sync.RWMutex
	// Padding to avoid multiple locks being on the same cache line.
	_ [40]byte
}

func newStripeSeries() *stripeSeries {
	s := &stripeSeries{}

	for i := range s.series {
		s.series[i] = map[uint64]*memSeries{}
	}
	for i := range s.hashes {
		s.hashes[i] = seriesHashmap{}
	}
	s.groups = make(map[uint64]*memSeries)
	return s
}

// return <set of removed series, number of removed groups>
// NOTICE(Alec), this will only block the current truncating group.
func (s *stripeSeries) gc(mint int64) (map[uint64]struct{}, int) {
	var (
		rmSeries    = map[uint64]struct{}{}
		rmGroupsNum = 0
	)

	// Quick snapshot for the groups.
	s.gLock.RLock()
	tempRefs := make([]uint64, 0, len(s.groups))
	tempSeries := make([]*memSeries, 0, len(s.groups))
	for gref, series := range s.groups {
		tempRefs = append(tempRefs, gref)
		tempSeries = append(tempSeries, series)
	}
	s.gLock.RUnlock()

	for idx := 0; idx < len(tempRefs); idx++ {
		tempSeries[idx].Lock()
		tempSeries[idx].truncateBefore(mint)
		tempSeries[idx].Unlock()

		// remove empty GroupMemSeries.
		if tempSeries[idx].groupChunk.Count() == 0 {
			// Remove all GroupMemSeries in series according to the ref.
			for ref, _ := range tempSeries[idx].refIndices {
				rmSeries[ref] = struct{}{}

				i := ref & stripeMask
				s.locks[i].Lock()
				delete(s.series[i], ref)
				s.locks[i].Unlock()
			}

			// Remove all GroupMemSeries in hashes according to the hash.
			for _, lset := range tempSeries[idx].labels {
				h := lset.Hash()
				i := h & stripeMask
				s.locks[i].Lock()
				s.hashes[i].delByHash(h)
				s.locks[i].Unlock()	
			}

			s.gLock.Lock()
			delete(s.groups, tempRefs[idx])
			s.gLock.Unlock()
			rmGroupsNum += 1
		}
	}

	return rmSeries, rmGroupsNum
}

func (s *stripeSeries) getByID(id uint64) *memSeries {
	i := id & stripeMask

	s.locks[i].RLock()
	series := s.series[i][id]
	s.locks[i].RUnlock()

	return series
}

func (s *stripeSeries) getByHash(hash uint64, lset labels.Labels) *memSeries {
	i := hash & stripeMask

	s.locks[i].RLock()
	series := s.hashes[i].get(hash, lset)
	s.locks[i].RUnlock()

	return series
}

func (s *stripeSeries) getGroup(groupRef uint64) *memSeries {
	s.gLock.RLock()
	defer s.gLock.RUnlock()
	return s.groups[groupRef]
}

// Return <GroupMemSeries, if the series being set>.
// Need to provide the hash and the series id(not group id).
func (s *stripeSeries) getOrSet(hash uint64, ref uint64, series *memSeries) (*memSeries, bool) {
	i := hash & stripeMask

	s.locks[i].Lock()

	if prev := s.hashes[i].get(hash, series.labels[series.refIndices[ref]]); prev != nil {
		s.locks[i].Unlock()
		return prev, false
	}
	s.hashes[i].set(hash, series)
	s.locks[i].Unlock()

	i = ref & stripeMask

	s.locks[i].Lock()
	s.series[i][ref] = series
	s.locks[i].Unlock()

	return series, true
}

func (s *stripeSeries) set(hash uint64, ref uint64, series *memSeries) {
	i := hash & stripeMask

	s.locks[i].Lock()
	s.hashes[i].set(hash, series)
	s.locks[i].Unlock()

	i = ref & stripeMask

	s.locks[i].Lock()
	s.series[i][ref] = series
	s.locks[i].Unlock()
}

func (s *stripeSeries) addGroup(labels []labels.Labels, groupRef uint64, ids []uint64) *memSeries {
	s.gLock.RLock()
	if g, ok := s.groups[groupRef]; ok {
		s.gLock.RUnlock()
		return g
	}
	s.gLock.RUnlock()
	g := newMemSeries(labels, groupRef, ids)
	s.gLock.Lock()
	s.groups[groupRef] = g
	s.gLock.Unlock()
	for i, id := range ids {
		s.set(labels[i].Hash(), id, g)
	}
	return g
}

func (s *stripeSeries) getSortedGroup() []uint64 {
	s.gLock.RLock()
	grps := make([]*memSeries, 0, len(s.groups))

	for _, g := range s.groups {
		grps = append(grps, g)
	}
	s.gLock.RUnlock()

	sort.Slice(grps, func(i, j int) bool {
		return labels.Compare(grps[i].labels[0], grps[i].labels[0]) < 0
	})

	rg := make([]uint64, 0, len(grps))
	for _, m := range grps {
		rg = append(rg, m.groupRef)
	}
	return rg
}

func (s *stripeSeries) getGroupPostings(groupRef uint64) []uint64 {
	m := s.getGroup(groupRef)
	if m == nil {
		return nil
	}
	return m.ids
}

type sample struct {
	t int64
	v float64
}

func (s sample) T() int64 {
	return s.t
}

func (s sample) V() float64 {
	return s.v
}

// memSeries is the in-memory representation of a group of series. None of its methods
// are goroutine safe and it is the caller's responsibility to lock it.
type memSeries struct {
	sync.Mutex

	groupRef   uint64
	refIndices map[uint64]int
	labels     []labels.Labels
	ids        []uint64
	groupChunk *chunkenc.GroupMemoryChunk1
	
	maxTime_ int64

	pendingCommit bool // Whether there are samples waiting to be committed to this series.

	app chunkenc.Appender // Current appender for the chunk.
}

// NOTE(Alec)
// 1. The order of labels and ids should be the same.
// 2. The length of labels and ids should be equal.
//
// Do not pass reusable variables to labels and ids.
//
func newMemSeries(labels []labels.Labels, gid uint64, ids []uint64) *memSeries {
	s := &memSeries{
		groupRef:   gid,
		refIndices: make(map[uint64]int),
		labels:     labels,
		ids:        ids,
		groupChunk: chunkenc.NewGroupMemoryChunk1(len(labels)),
		maxTime_:   math.MinInt64,
	}
	s.app, _ = s.groupChunk.Appender()
	for i, id := range ids {
		s.refIndices[id] = i
	}
	return s
}

func (s *memSeries) minTime() int64 {
	return s.groupChunk.MinTime()
}

func (s *memSeries) maxTime() int64 {
	return s.maxTime_
}

// Return false if out of bounds.
func (s *memSeries) appendGroup(t int64, vals []float64) bool {
	if s.maxTime_ >= t {
		return false;
	}
	s.app.AppendGroup(t, vals)
	s.maxTime_ = t
	return true
}

func (s *memSeries) reset() {
	s.groupChunk = chunkenc.NewGroupMemoryChunk1(len(s.labels))
	s.pendingCommit = false
	s.app, _ = s.groupChunk.Appender()
	s.maxTime_ = math.MinInt64
}

// appendable checks whether the given sample is valid for appending to the series.
func (s *memSeries) appendable(t int64) error {
	if t > s.maxTime_ {
		return nil
	} else {
		return ErrOutOfOrderSample
	}
}

// series ref.
func (s *memSeries) chunk(id uint64) *chunkenc.Meta {
	m := chunkenc.Meta{
		Ref:       s.groupRef,
		SeriesRef: uint64(s.refIndices[id]),
		Chunk:     s.groupChunk,
		MinTime:   s.groupChunk.MinTime(),
		MaxTime:   s.maxTime_,
	}
	return &m
}

// Return removed samples.
func (s *memSeries) truncateBefore(mint int64) int {
	if mint <= s.groupChunk.MinTime() {
		return 0
	}
	numT := s.groupChunk.Count()
	groupChunk := chunkenc.NewGroupMemoryChunk1(len(s.labels))
	s.app, _ = groupChunk.Appender()
	iterators := make([]chunkenc.Iterator, 0, len(s.labels))
	for i := 0; i < len(s.labels); i++ {
		iterators = append(iterators, s.groupChunk.IteratorGroup(mint, i))
	}
	values := make([]float64, 0, len(s.labels))
	for iterators[0].Next() {
		values = values[:0]
		t, v := iterators[0].At()
		values = append(values, v)
		for i := 1; i < len(iterators); i++ {
			iterators[i].Next()
			_, v = iterators[i].At()
			values = append(values, v)
		}
		s.app.AppendGroup(t, values)
	}

	s.groupChunk = groupChunk

	// Means the new chunk is empty.
	if s.groupChunk.MinTime() == math.MaxInt64 {
		s.maxTime_ = math.MinInt64
	}
	return (numT - s.groupChunk.Count()) * len(s.labels)
}

func (s *memSeries) iterator(t int64, ref uint64) chunkenc.Iterator {
	if i, ok := s.refIndices[ref]; ok {
		return s.groupChunk.IteratorGroup(t, i)
	}
	return chunkenc.NewNopIterator()
}

// Please check this in branch gmc2. Master branch still uses gmc1.
// // memSeries using GMC2.
// type memSeries struct {
// 	sync.Mutex

// 	groupRef   uint64
// 	refIndices map[uint64]int
// 	labels     []labels.Labels
// 	ids        []uint64
// 	groupChunk *chunkenc.GroupMemoryChunk2
	
// 	maxTime_ int64

// 	pendingCommit bool // Whether there are samples waiting to be committed to this series.

// 	app chunkenc.Appender // Current appender for the chunk.
// }

// // NOTE(Alec)
// // 1. The order of labels and ids should be the same.
// // 2. The length of labels and ids should be equal.
// //
// // Do not pass reusable variables to labels and ids.
// //
// func newMemSeries(labels []labels.Labels, gid uint64, ids []uint64) *memSeries {
// 	s := &memSeries{
// 		groupRef:   gid,
// 		refIndices: make(map[uint64]int),
// 		labels:     labels,
// 		ids:        ids,
// 		groupChunk: chunkenc.NewGroupMemoryChunk2(len(labels)),
// 		maxTime_:   math.MinInt64,
// 	}
// 	s.app, _ = s.groupChunk.Appender()
// 	for i, id := range ids {
// 		s.refIndices[id] = i
// 	}
// 	return s
// }

// func (s *memSeries) minTime() int64 {
// 	return s.groupChunk.MinTime()
// }

// func (s *memSeries) maxTime() int64 {
// 	return s.maxTime_
// }

// // Return false if out of bounds.
// func (s *memSeries) appendGroup(t int64, vals []float64) bool {
// 	if s.maxTime_ >= t {
// 		return false;
// 	}
// 	s.app.AppendGroup(t, vals)
// 	s.maxTime_ = t
// 	return true
// }

// func (s *memSeries) reset() {
// 	s.groupChunk = chunkenc.NewGroupMemoryChunk2(len(s.labels))
// 	s.pendingCommit = false
// 	s.app, _ = s.groupChunk.Appender()
// 	s.maxTime_ = math.MinInt64
// }

// // appendable checks whether the given sample is valid for appending to the series.
// func (s *memSeries) appendable(t int64) error {
// 	if t > s.maxTime_ {
// 		return nil
// 	} else {
// 		return ErrOutOfOrderSample
// 	}
// }

// // series ref.
// func (s *memSeries) chunk(id uint64) *chunkenc.Meta {
// 	m := chunkenc.Meta{
// 		Ref:       s.groupRef,
// 		SeriesRef: uint64(s.refIndices[id]),
// 		Chunk:     s.groupChunk,
// 		MinTime:   s.groupChunk.MinTime(),
// 		MaxTime:   s.maxTime_,
// 	}
// 	return &m
// }

// // Return removed samples.
// func (s *memSeries) truncateBefore(mint int64) int {
// 	if mint <= s.groupChunk.MinTime() {
// 		return 0
// 	}
// 	groupChunk, deleted := s.groupChunk.TruncateBefore(mint)
// 	if deleted == 0 {
// 		return 0
// 	}

// 	s.groupChunk = groupChunk

// 	// Means the new chunk is empty.
// 	if s.groupChunk.MinTime() == math.MaxInt64 {
// 		s.maxTime_ = math.MinInt64
// 	}
// 	return deleted * len(s.labels)
// }

// func (s *memSeries) iterator(t int64, ref uint64) chunkenc.Iterator {
// 	if i, ok := s.refIndices[ref]; ok {
// 		return s.groupChunk.IteratorGroup(t, i)
// 	}
// 	return chunkenc.NewNopIterator()
// }

type memChunk struct {
	chunk            chunkenc.Chunk
	minTime, maxTime int64
}

// Returns true if the chunk overlaps [mint, maxt].
func (mc *memChunk) OverlapsClosedInterval(mint, maxt int64) bool {
	return mc.minTime <= maxt && mint <= mc.maxTime
}

type stringset map[string]struct{}

func (ss stringset) set(s string) {
	ss[s] = struct{}{}
}

func (ss stringset) String() string {
	return strings.Join(ss.slice(), ",")
}

func (ss stringset) slice() []string {
	slice := make([]string, 0, len(ss))
	for k := range ss {
		slice = append(slice, k)
	}
	sort.Strings(slice)
	return slice
}
