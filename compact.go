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
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/naivewong/tsdb-group/chunkenc"
	"github.com/naivewong/tsdb-group/chunks"
	tsdb_errors "github.com/naivewong/tsdb-group/errors"
	"github.com/naivewong/tsdb-group/fileutil"
	"github.com/naivewong/tsdb-group/index"
	"github.com/naivewong/tsdb-group/labels"
)

// ExponentialBlockRanges returns the time ranges based on the stepSize.
func ExponentialBlockRanges(minSize int64, steps, stepSize int) []int64 {
	ranges := make([]int64, 0, steps)
	curRange := minSize
	for i := 0; i < steps; i++ {
		ranges = append(ranges, curRange)
		curRange = curRange * int64(stepSize)
	}

	return ranges
}

// Compactor provides compaction against an underlying storage
// of time series data.
type Compactor interface {
	// Plan returns a set of directories that can be compacted concurrently.
	// The directories can be overlapping.
	// Results returned when compactions are in progress are undefined.
	Plan(dir string) ([]string, error)

	// Write persists a Block into a directory.
	// No Block is written when resulting Block has 0 samples, and returns empty ulid.ULID{}.
	Write(dest string, b BlockReader, mint, maxt int64, parent *BlockMeta) (ulid.ULID, error)

	// Compact runs compaction against the provided directories. Must
	// only be called concurrently with results of Plan().
	// Can optionally pass a list of already open blocks,
	// to avoid having to reopen them.
	// When resulting Block has 0 samples
	//  * No block is written.
	//  * The source dirs are marked Deletable.
	//  * Returns empty ulid.ULID{}.
	Compact(dest string, dirs []string, open []*Block) (ulid.ULID, error)
}

// LeveledCompactor implements the Compactor interface.
type LeveledCompactor struct {
	metrics   *compactorMetrics
	logger    log.Logger
	ranges    []int64
	chunkPool chunkenc.Pool
	ctx       context.Context
}

type compactorMetrics struct {
	ran               prometheus.Counter
	populatingBlocks  prometheus.Gauge
	overlappingBlocks prometheus.Counter
	duration          prometheus.Histogram
	chunkSize         prometheus.Histogram
	chunkSamples      prometheus.Histogram
	chunkRange        prometheus.Histogram
}

func newCompactorMetrics(r prometheus.Registerer) *compactorMetrics {
	m := &compactorMetrics{}

	m.ran = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_compactions_total",
		Help: "Total number of compactions that were executed for the partition.",
	})
	m.populatingBlocks = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_compaction_populating_block",
		Help: "Set to 1 when a block is currently being written to the disk.",
	})
	m.overlappingBlocks = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_vertical_compactions_total",
		Help: "Total number of compactions done on overlapping blocks.",
	})
	m.duration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "prometheus_tsdb_compaction_duration_seconds",
		Help:    "Duration of compaction runs",
		Buckets: prometheus.ExponentialBuckets(1, 2, 10),
	})
	m.chunkSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "prometheus_tsdb_compaction_chunk_size_bytes",
		Help:    "Final size of chunks on their first compaction",
		Buckets: prometheus.ExponentialBuckets(32, 1.5, 12),
	})
	m.chunkSamples = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "prometheus_tsdb_compaction_chunk_samples",
		Help:    "Final number of samples on their first compaction",
		Buckets: prometheus.ExponentialBuckets(4, 1.5, 12),
	})
	m.chunkRange = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "prometheus_tsdb_compaction_chunk_range_seconds",
		Help:    "Final time range of chunks on their first compaction",
		Buckets: prometheus.ExponentialBuckets(100, 4, 10),
	})

	if r != nil {
		r.MustRegister(
			m.ran,
			m.populatingBlocks,
			m.overlappingBlocks,
			m.duration,
			m.chunkRange,
			m.chunkSamples,
			m.chunkSize,
		)
	}
	return m
}

// NewLeveledCompactor returns a LeveledCompactor.
func NewLeveledCompactor(ctx context.Context, r prometheus.Registerer, l log.Logger, ranges []int64, pool chunkenc.Pool) (*LeveledCompactor, error) {
	if len(ranges) == 0 {
		return nil, errors.Errorf("at least one range must be provided")
	}
	if pool == nil {
		pool = chunkenc.NewPool()
	}
	if l == nil {
		l = log.NewNopLogger()
	}
	return &LeveledCompactor{
		ranges:    ranges,
		chunkPool: pool,
		logger:    l,
		metrics:   newCompactorMetrics(r),
		ctx:       ctx,
	}, nil
}

type dirMeta struct {
	dir  string
	meta *BlockMeta
}

// Plan returns a list of compactable blocks in the provided directory.
func (c *LeveledCompactor) Plan(dir string) ([]string, error) {
	dirs, err := blockDirs(dir)
	if err != nil {
		return nil, err
	}
	if len(dirs) < 1 {
		return nil, nil
	}

	var dms []dirMeta
	for _, dir := range dirs {
		meta, _, err := readMetaFile(dir)
		if err != nil {
			return nil, err
		}
		dms = append(dms, dirMeta{dir, meta})
	}
	return c.plan(dms)
}

func (c *LeveledCompactor) plan(dms []dirMeta) ([]string, error) {
	sort.Slice(dms, func(i, j int) bool {
		return dms[i].meta.MinTime < dms[j].meta.MinTime
	})

	res := c.selectOverlappingDirs(dms)
	if len(res) > 0 {
		return res, nil
	}
	// No overlapping blocks, do compaction the usual way.
	// We do not include a recently created block with max(minTime), so the block which was just created from WAL.
	// This gives users a window of a full block size to piece-wise backup new data without having to care about data overlap.
	dms = dms[:len(dms)-1]

	for _, dm := range c.selectDirs(dms) {
		res = append(res, dm.dir)
	}
	if len(res) > 0 {
		return res, nil
	}

	// Compact any blocks with big enough time range that have >5% tombstones.
	for i := len(dms) - 1; i >= 0; i-- {
		meta := dms[i].meta
		if meta.MaxTime-meta.MinTime < c.ranges[len(c.ranges)/2] {
			break
		}
		if float64(meta.Stats.NumTombstones)/float64(meta.Stats.NumSeries+1) > 0.05 {
			return []string{dms[i].dir}, nil
		}
	}

	return nil, nil
}

// selectDirs returns the dir metas that should be compacted into a single new block.
// If only a single block range is configured, the result is always nil.
func (c *LeveledCompactor) selectDirs(ds []dirMeta) []dirMeta {
	if len(c.ranges) < 2 || len(ds) < 1 {
		return nil
	}

	highTime := ds[len(ds)-1].meta.MinTime

	for _, iv := range c.ranges[1:] {
		parts := splitByRange(ds, iv)
		if len(parts) == 0 {
			continue
		}

	Outer:
		for _, p := range parts {
			// Do not select the range if it has a block whose compaction failed.
			for _, dm := range p {
				if dm.meta.Compaction.Failed {
					continue Outer
				}
			}

			mint := p[0].meta.MinTime
			maxt := p[len(p)-1].meta.MaxTime
			// Pick the range of blocks if it spans the full range (potentially with gaps)
			// or is before the most recent block.
			// This ensures we don't compact blocks prematurely when another one of the same
			// size still fits in the range.
			if (maxt-mint == iv || maxt <= highTime) && len(p) > 1 {
				return p
			}
		}
	}

	return nil
}

// selectOverlappingDirs returns all dirs with overlapping time ranges.
// It expects sorted input by mint and returns the overlapping dirs in the same order as received.
func (c *LeveledCompactor) selectOverlappingDirs(ds []dirMeta) []string {
	if len(ds) < 2 {
		return nil
	}
	var overlappingDirs []string
	globalMaxt := ds[0].meta.MaxTime
	for i, d := range ds[1:] {
		if d.meta.MinTime < globalMaxt {
			if len(overlappingDirs) == 0 { // When it is the first overlap, need to add the last one as well.
				overlappingDirs = append(overlappingDirs, ds[i].dir)
			}
			overlappingDirs = append(overlappingDirs, d.dir)
		} else if len(overlappingDirs) > 0 {
			break
		}
		if d.meta.MaxTime > globalMaxt {
			globalMaxt = d.meta.MaxTime
		}
	}
	return overlappingDirs
}

// splitByRange splits the directories by the time range. The range sequence starts at 0.
//
// For example, if we have blocks [0-10, 10-20, 50-60, 90-100] and the split range tr is 30
// it returns [0-10, 10-20], [50-60], [90-100].
func splitByRange(ds []dirMeta, tr int64) [][]dirMeta {
	var splitDirs [][]dirMeta

	for i := 0; i < len(ds); {
		var (
			group []dirMeta
			t0    int64
			m     = ds[i].meta
		)
		// Compute start of aligned time range of size tr closest to the current block's start.
		if m.MinTime >= 0 {
			t0 = tr * (m.MinTime / tr)
		} else {
			t0 = tr * ((m.MinTime - tr + 1) / tr)
		}
		// Skip blocks that don't fall into the range. This can happen via mis-alignment or
		// by being the multiple of the intended range.
		if m.MaxTime > t0+tr {
			i++
			continue
		}

		// Add all dirs to the current group that are within [t0, t0+tr].
		for ; i < len(ds); i++ {
			// Either the block falls into the next range or doesn't fit at all (checked above).
			if ds[i].meta.MaxTime > t0+tr {
				break
			}
			group = append(group, ds[i])
		}

		if len(group) > 0 {
			splitDirs = append(splitDirs, group)
		}
	}

	return splitDirs
}

func compactBlockMetas(uid ulid.ULID, blocks ...*BlockMeta) *BlockMeta {
	res := &BlockMeta{
		ULID:    uid,
		MinTime: blocks[0].MinTime,
	}

	sources := map[ulid.ULID]struct{}{}
	// For overlapping blocks, the Maxt can be
	// in any block so we track it globally.
	maxt := int64(math.MinInt64)

	for _, b := range blocks {
		if b.MaxTime > maxt {
			maxt = b.MaxTime
		}
		if b.Compaction.Level > res.Compaction.Level {
			res.Compaction.Level = b.Compaction.Level
		}
		for _, s := range b.Compaction.Sources {
			sources[s] = struct{}{}
		}
		res.Compaction.Parents = append(res.Compaction.Parents, BlockDesc{
			ULID:    b.ULID,
			MinTime: b.MinTime,
			MaxTime: b.MaxTime,
		})
	}
	res.Compaction.Level++

	for s := range sources {
		res.Compaction.Sources = append(res.Compaction.Sources, s)
	}
	sort.Slice(res.Compaction.Sources, func(i, j int) bool {
		return res.Compaction.Sources[i].Compare(res.Compaction.Sources[j]) < 0
	})

	res.MaxTime = maxt
	return res
}

// Compact creates a new block in the compactor's directory from the blocks in the
// provided directories.
func (c *LeveledCompactor) Compact(dest string, dirs []string, open []*Block) (uid ulid.ULID, err error) {
	var (
		blocks []BlockReader
		bs     []*Block
		metas  []*BlockMeta
		uids   []string
	)
	start := time.Now()

	for _, d := range dirs {
		meta, _, err := readMetaFile(d)
		if err != nil {
			return uid, err
		}

		var b *Block

		// Use already open blocks if we can, to avoid
		// having the index data in memory twice.
		for _, o := range open {
			if meta.ULID == o.Meta().ULID {
				b = o
				break
			}
		}

		if b == nil {
			var err error
			b, err = OpenBlock(c.logger, d, c.chunkPool)
			if err != nil {
				return uid, err
			}
			defer b.Close()
		}

		metas = append(metas, meta)
		blocks = append(blocks, b)
		bs = append(bs, b)
		uids = append(uids, meta.ULID.String())
	}

	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	uid = ulid.MustNew(ulid.Now(), entropy)

	meta := compactBlockMetas(uid, metas...)
	err = c.write(dest, meta, blocks...)
	if err == nil {
		if meta.Stats.NumSamples == 0 {
			for _, b := range bs {
				b.meta.Compaction.Deletable = true
				n, err := writeMetaFile(c.logger, b.dir, &b.meta)
				if err != nil {
					level.Error(c.logger).Log(
						"msg", "Failed to write 'Deletable' to meta file after compaction",
						"ulid", b.meta.ULID,
					)
				}
				b.numBytesMeta = n
			}
			uid = ulid.ULID{}
			level.Info(c.logger).Log(
				"msg", "compact blocks resulted in empty block",
				"count", len(blocks),
				"sources", fmt.Sprintf("%v", uids),
				"duration", time.Since(start),
			)
		} else {
			level.Info(c.logger).Log(
				"msg", "compact blocks",
				"count", len(blocks),
				"mint", meta.MinTime,
				"maxt", meta.MaxTime,
				"ulid", meta.ULID,
				"sources", fmt.Sprintf("%v", uids),
				"duration", time.Since(start),
			)
		}
		return uid, nil
	}

	var merr tsdb_errors.MultiError
	merr.Add(err)
	if err != context.Canceled {
		for _, b := range bs {
			if err := b.setCompactionFailed(); err != nil {
				merr.Add(errors.Wrapf(err, "setting compaction failed for block: %s", b.Dir()))
			}
		}
	}

	return uid, merr
}

func (c *LeveledCompactor) Write(dest string, b BlockReader, mint, maxt int64, parent *BlockMeta) (ulid.ULID, error) {
	start := time.Now()

	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	uid := ulid.MustNew(ulid.Now(), entropy)

	meta := &BlockMeta{
		ULID:    uid,
		MinTime: mint,
		MaxTime: maxt,
	}
	meta.Compaction.Level = 1
	meta.Compaction.Sources = []ulid.ULID{uid}

	if parent != nil {
		meta.Compaction.Parents = []BlockDesc{
			{ULID: parent.ULID, MinTime: parent.MinTime, MaxTime: parent.MaxTime},
		}
	}

	err := c.write(dest, meta, b)
	if err != nil {
		return uid, err
	}

	if meta.Stats.NumSamples == 0 {
		return ulid.ULID{}, nil
	}

	level.Info(c.logger).Log(
		"msg", "write block",
		"mint", meta.MinTime,
		"maxt", meta.MaxTime,
		"ulid", meta.ULID,
		"duration", time.Since(start),
	)
	return uid, nil
}

// instrumentedChunkWriter is used for level 1 compactions to record statistics
// about compacted chunks.
type instrumentedChunkWriter struct {
	ChunkWriter

	size    prometheus.Histogram
	samples prometheus.Histogram
	trange  prometheus.Histogram
}

func (w *instrumentedChunkWriter) WriteChunks(chunks ...chunkenc.Meta) error {
	for _, c := range chunks {
		w.size.Observe(float64(len(c.Chunk.Bytes())))
		w.samples.Observe(float64(c.Chunk.NumSamples()))
		w.trange.Observe(float64(c.MaxTime - c.MinTime))
	}
	return w.ChunkWriter.WriteChunks(chunks...)
}

// write creates a new block that is the union of the provided blocks into dir.
// It cleans up all files of the old blocks after completing successfully.
func (c *LeveledCompactor) write(dest string, meta *BlockMeta, blocks ...BlockReader) (err error) {
	dir := filepath.Join(dest, meta.ULID.String())
	tmp := dir + ".tmp"
	var closers []io.Closer
	defer func(t time.Time) {
		var merr tsdb_errors.MultiError
		merr.Add(err)
		merr.Add(closeAll(closers))
		err = merr.Err()

		// RemoveAll returns no error when tmp doesn't exist so it is safe to always run it.
		if err := os.RemoveAll(tmp); err != nil {
			level.Error(c.logger).Log("msg", "removed tmp folder after failed compaction", "err", err.Error())
		}
		c.metrics.ran.Inc()
		c.metrics.duration.Observe(time.Since(t).Seconds())
	}(time.Now())

	if err = os.RemoveAll(tmp); err != nil {
		return err
	}

	if err = os.MkdirAll(tmp, 0777); err != nil {
		return err
	}

	// Populate chunk and index files into temporary directory with
	// data of all blocks.
	var chunkw ChunkWriter

	chunkw, err = chunks.NewWriter(chunkDir(tmp))
	if err != nil {
		return errors.Wrap(err, "open chunk writer")
	}
	closers = append(closers, chunkw)
	// Record written chunk sizes on level 1 compactions.
	if meta.Compaction.Level == 1 {
		chunkw = &instrumentedChunkWriter{
			ChunkWriter: chunkw,
			size:        c.metrics.chunkSize,
			samples:     c.metrics.chunkSamples,
			trange:      c.metrics.chunkRange,
		}
	}

	indexw, err := index.NewWriter(filepath.Join(tmp, indexFilename))
	if err != nil {
		return errors.Wrap(err, "open index writer")
	}
	closers = append(closers, indexw)

	if err := c.populateBlock(blocks, meta, indexw, chunkw); err != nil {
		return errors.Wrap(err, "write compaction")
	}

	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	default:
	}

	// We are explicitly closing them here to check for error even
	// though these are covered under defer. This is because in Windows,
	// you cannot delete these unless they are closed and the defer is to
	// make sure they are closed if the function exits due to an error above.
	var merr tsdb_errors.MultiError
	for _, w := range closers {
		merr.Add(w.Close())
	}
	closers = closers[:0] // Avoid closing the writers twice in the defer.
	if merr.Err() != nil {
		return merr.Err()
	}

	// Populated block is empty, so exit early.
	if meta.Stats.NumSamples == 0 {
		return nil
	}

	if _, err = writeMetaFile(c.logger, tmp, meta); err != nil {
		return errors.Wrap(err, "write merged meta")
	}

	// Create an empty tombstones file.
	if _, err := writeTombstoneFile(c.logger, tmp, newMemTombstones()); err != nil {
		return errors.Wrap(err, "write new tombstones file")
	}

	df, err := fileutil.OpenDir(tmp)
	if err != nil {
		return errors.Wrap(err, "open temporary block dir")
	}
	defer func() {
		if df != nil {
			df.Close()
		}
	}()

	if err := df.Sync(); err != nil {
		return errors.Wrap(err, "sync temporary dir file")
	}

	// Close temp dir before rename block dir (for windows platform).
	if err = df.Close(); err != nil {
		return errors.Wrap(err, "close temporary dir")
	}
	df = nil

	// Block successfully written, make visible and remove old ones.
	if err := fileutil.Replace(tmp, dir); err != nil {
		return errors.Wrap(err, "rename block dir")
	}

	return nil
}

type tempCSM struct {
	id   uint64
	lset labels.Labels
	chks []chunkenc.Meta
}

func newTempCSM(id uint64, lset labels.Labels, chks []chunkenc.Meta) *tempCSM {
	return &tempCSM{id: id, lset: lset, chks: chks}
}

// populateBlock fills the index and chunk writers with new data gathered as the union
// of the provided blocks. It returns meta information for the new block.
// It expects sorted blocks input by mint.
func (c *LeveledCompactor) populateBlock(blocks []BlockReader, meta *BlockMeta, indexw IndexWriter, chunkw ChunkWriter) (err error) {
	if len(blocks) == 0 {
		return errors.New("cannot populate block from no readers")
	}

	var (
		gsets       = &GroupChunkSeriesSets{}
		allSymbols  = make(map[string]struct{}, 1<<16)
		closers     = []io.Closer{}
		overlapping bool
	)
	defer func() {
		var merr tsdb_errors.MultiError
		merr.Add(err)
		merr.Add(closeAll(closers))
		err = merr.Err()
		c.metrics.populatingBlocks.Set(0)
	}()
	c.metrics.populatingBlocks.Set(1)

	globalMaxt := blocks[0].MaxTime()
	for i, b := range blocks {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		default:
		}

		if !overlapping {
			if i > 0 && b.MinTime() < globalMaxt {
				c.metrics.overlappingBlocks.Inc()
				overlapping = true
				level.Warn(c.logger).Log("msg", "found overlapping blocks during compaction", "ulid", meta.ULID)
			}
			if b.MaxTime() > globalMaxt {
				globalMaxt = b.MaxTime()
			}
		}

		indexr, err := b.Index()
		if err != nil {
			return errors.Wrapf(err, "open index reader for block %s", b)
		}
		closers = append(closers, indexr)

		chunkr, err := b.Chunks()
		if err != nil {
			return errors.Wrapf(err, "open chunk reader for block %s", b)
		}
		closers = append(closers, chunkr)

		tombsr, err := b.Tombstones()
		if err != nil {
			return errors.Wrapf(err, "open tombstone reader for block %s", b)
		}
		closers = append(closers, tombsr)

		symbols, err := indexr.Symbols()
		if err != nil {
			return errors.Wrap(err, "read symbols")
		}
		for s := range symbols {
			allSymbols[s] = struct{}{}
		}

		all, err := indexr.GroupPostings(index.AllGroupPostings)
		if err != nil {
			return errors.Wrap(err, "get all group postings")
		}
		all = indexr.SortedGroupPostings(all)

		// Append block to ChunkSeriesSets.
		s := newCompactionSeriesSet(indexr, chunkr, tombsr, all)
		gsets.PushBack(s)
	}

	// Create compactionMerger.
	cm, err := newCompactionMerger(gsets.sets)
	if err != nil {
		return errors.Wrap(err, "create compactionMerger")
	}

	// We fully rebuild the postings list index from merged series.
	var (
		postings           = index.NewMemPostings()
		values             = map[string]stringset{}
		id                 = uint64(0)
		groupPostingsIds   = []uint64{}
		groupPostingsSizes = []int{}
		tempCSMs           = []*tempCSM{}
	)

	// STEP 1 in index writer.
	if err := indexw.AddSymbols(allSymbols); err != nil {
		return errors.Wrap(err, "add symbols")
	}

	for cm.Next() {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		default:
		}

		lsets, chks, dranges := cm.At() // The chunks here are not fully deleted.
		
		if overlapping {
			// If blocks are overlapping, it is possible to have unsorted chunks.
			// TODO(Alec), more efficient.
			for i := 0; i < len(chks); i++ {
				sort.Slice(chks[i], func(j, k int) bool { return chks[i][j].MinTime < chks[i][j].MinTime })
			}
		}

		// Skip the series with all deleted chunks.
		if len(chks) == 0 {
			continue
		}

		for i := 0; i < len(chks[0]); i++ {
			if chks[0][i].MinTime < meta.MinTime || chks[0][i].MaxTime > meta.MaxTime {
				return errors.Errorf("found chunk with minTime: %d maxTime: %d outside of compacted minTime: %d maxTime: %d",
					chks[0][i].MinTime, chks[0][i].MaxTime, meta.MinTime, meta.MaxTime)
			}

			if len(dranges) > 0 {
				// Re-encode the chunk to not have deleted values.
				if !chks[0][i].OverlapsClosedInterval(dranges[0].Mint, dranges[len(dranges)-1].Maxt) {
					continue
				}
				newChunk := chunkenc.NewGroupMemoryChunk1(len(chks))
				app, err := newChunk.Appender()
				if err != nil {
					return err
				}

				tempChks := make([][]chunkenc.Meta, 0, len(chks))
				for j := 0; j < len(chks); j++ {
					tempChks = append(tempChks, []chunkenc.Meta{chks[j][i]})
				}

				// Get groupChunkSeriesIterator.
				gcsi := newGroupChunkSeriesIterator(tempChks, dranges, tempChks[0][0].MinTime, tempChks[0][0].MaxTime)
				for gcsi.Next() {
					ts, vals := gcsi.At()
					app.AppendGroup(ts, vals)
				}

				for j := 0; j < len(chks); j++ {
					// NOTE(Alec), need to update series_ref because the GroupHeadChunk use 
					// a different series_ref from GMC1.
					chks[j][i].SeriesRef = uint64(j)
					chks[j][i].Chunk = newChunk
				}
			}
		}

		mergedChks := chks
		if overlapping {
			mergedChks, err = MergeOverlappingGroupChunks(chks)
			if err != nil {
				return errors.Wrap(err, "merge overlapping group chunks")
			}
		}

		for i := 0; i < len(mergedChks[0]); i++ {
			tempChks := make([]chunkenc.Meta, 0, len(mergedChks))
			for j := 0; j < len(mergedChks); j++ {
				tempChks = append(tempChks, mergedChks[j][i])
			}
			if err := chunkw.WriteChunks(tempChks...); err != nil {
				return errors.Wrap(err, "write chunks")
			}
			for j := 0; j < len(mergedChks); j++ {
				mergedChks[j][i] = tempChks[j]
			}
			meta.Stats.NumSamples += uint64(mergedChks[0][i].Chunk.NumSamples())
		}

		meta.Stats.NumChunks += uint64(len(mergedChks[0]))
		meta.Stats.NumSeries += uint64(len(mergedChks))

		for j := 0; j < len(mergedChks); j++ {
			for _, l := range lsets[j] {
				valset, ok := values[l.Name]
				if !ok {
					valset = stringset{}
					values[l.Name] = valset
				}
				valset.set(l.Value)
			}
			
			postings.Add(id+uint64(j), lsets[j])
			
			// Add ChunkSeriesMeta (each row represents a series).
			tempCSMs = append(tempCSMs, newTempCSM(id+uint64(j), lsets[j], mergedChks[j]))
		}
		groupPostingsIds = append(groupPostingsIds, id)
		groupPostingsSizes = append(groupPostingsSizes, len(lsets))
		id += uint64(len(lsets))
	}
	if cm.Err() != nil {
		return errors.Wrap(cm.Err(), "iterate compaction set")
	}

	// STEP 2 in index writer.
	sort.Slice(tempCSMs, func(i, j int) bool { return labels.Compare(tempCSMs[i].lset, tempCSMs[j].lset) < 0 })
	for _, csm := range tempCSMs {
		if err = indexw.AddSeries(csm.id, csm.lset, csm.chks...); err != nil {
			return errors.Wrap(err, "add series")
		}
	}

	// STEP 3 in index writer, write_label_index.
	s := make([]string, 0, 256)
	for n, v := range values {
		s = s[:0]

		for x := range v {
			s = append(s, x)
		}
		if err := indexw.WriteLabelIndex([]string{n}, s); err != nil {
			return errors.Wrap(err, "write label index")
		}
	}

	// STEP 4 in index writer, write_postings.
	for _, l := range postings.SortedKeys() {
		if err := indexw.WritePostings(l.Name, l.Value, postings.Get(l.Name, l.Value)); err != nil {
			return errors.Wrap(err, "write postings")
		}
	}

	// STEP 5 in index writer, write_group_postings.
	for i := 0; i < len(groupPostingsIds); i++ {
		ip := index.NewIncrementPostings(int(groupPostingsIds[i]), int(groupPostingsIds[i]) + groupPostingsSizes[i])
		if err := indexw.WriteGroupPostings(ip); err != nil {
			return errors.Wrap(err, "write group postings")
		}
	}

	return nil
}

// Implement GroupChunkSeriesSet interface in querier.go.
type compactionSeriesSet struct {
	p          index.Postings
	index      IndexReader
	chunks     ChunkReader
	tombstones TombstoneReader

	l         []labels.Labels
	c         [][]chunkenc.Meta
	intervals Intervals
	err       error
}

func newCompactionSeriesSet(i IndexReader, c ChunkReader, t TombstoneReader, p index.Postings) *compactionSeriesSet {
	return &compactionSeriesSet{
		index:      i,
		chunks:     c,
		tombstones: t,
		p:          p,
	}
}

func (c *compactionSeriesSet) Next() bool {
	if !c.p.Next() {
		return false
	}
	var err error

	c.intervals, err = c.tombstones.Get(c.p.At())
	if err != nil {
		c.err = errors.Wrap(err, "get tombstones")
		return false
	}

	// Clear the labels, chunk metas, and intervals.
	c.l = c.l[:0]
	c.c = c.c[:0]
	c.intervals = c.intervals[:0]

	grp, err := c.index.GroupPostings(c.p.At())
	if err != nil {
		c.err = errors.Wrapf(err, "get group %d", c.p.At())
		return false
	}

	for grp.Next() {
		tempL := labels.Labels{}
		tempC := []chunkenc.Meta{}
		if err = c.index.Series(grp.At(), &tempL, &tempC); err != nil {
			c.err = errors.Wrapf(err, "get series %d", grp.At())
			return false
		}
		c.l = append(c.l, tempL)
		c.c = append(c.c, tempC)
	}

	// Remove completely deleted chunks.
	if len(c.intervals) > 0 {
		i := 0
		for i < len(c.c[0]) {
			if !(Interval{c.c[0][i].MinTime, c.c[0][i].MaxTime}.isSubrange(c.intervals)) {
				// Pop chunks.
				for j := 0; j < len(c.c); j++ {
					c.c[j] = c.c[j][1:]
				}
			} else {
				i += 1
			}
		}
	}

	for i := range c.c {
		for j := range c.c[0] {
			chk := &c.c[i][j]

			chk.Chunk, err = c.chunks.Chunk(chk.Ref)
			if err != nil {
				c.err = errors.Wrapf(err, "chunk %d not found", chk.Ref)
				return false
			}
		}
	}

	return true
}

func (c *compactionSeriesSet) Err() error {
	if c.err != nil {
		return c.err
	}
	return c.p.Err()
}

func (c *compactionSeriesSet) At() ([]labels.Labels, [][]chunkenc.Meta, Intervals) {
	return c.l, c.c, c.intervals
}

type GroupChunkSeriesSets struct {
	sets []GroupChunkSeriesSet
}

func newGroupChunkSeriesSets(sets []GroupChunkSeriesSet) *GroupChunkSeriesSets {
	return &GroupChunkSeriesSets{sets: sets}
}

func (s *GroupChunkSeriesSets) PushBack(set GroupChunkSeriesSet) {
	s.sets = append(s.sets, set)
}

func (s *GroupChunkSeriesSets) Clear() {
	s.sets = s.sets[:0]
}

func (s *GroupChunkSeriesSets) Next() {
	temp := []GroupChunkSeriesSet{}
	for _, set := range s.sets {
		if set.Next() {
			temp = append(temp, set)
		}
	}
	s.sets = temp
}

// The ids passed-in should be in-bounds and ordered.
func (s *GroupChunkSeriesSets) NextWithIds(ids []int) {
	rm := []int{}
	for _, i := range ids {
		if !s.sets[i].Next() {
			rm = append(rm, i)
		}
	}
	if len(rm) == 0 {
		return
	}
	temp := make([]GroupChunkSeriesSet, 0, len(s.sets) - len(rm))
	idx := 0
	in := true
	for i, set := range s.sets {
		if in {
			if rm[idx] != i {
				temp = append(temp, set)
			} else {
				idx += 1
				if idx == len(rm) {
					in = false
				}
			}
		} else {
			temp = append(temp, set)
		}
	}
	s.sets = temp
}

// Merge a series of GroupChunkSeriesSet.
type compactionMerger struct {
	sets *GroupChunkSeriesSets
	ids  []int

	l         []labels.Labels
	c         [][]chunkenc.Meta
	intervals Intervals
}

func newCompactionMerger(sets []GroupChunkSeriesSet) (*compactionMerger, error) {
	s := newGroupChunkSeriesSets(sets)
	s.Next()
	if len(s.sets) == 0 {
		return nil, errors.Errorf("empty GroupChunkSeriesSets")
	}

	c := &compactionMerger{sets: s}

	return c, c.Err()
}

func (c *compactionMerger) nextHelper() bool {
	c.sets.NextWithIds(c.ids)

	c.l = c.l[:0]
	c.c = c.c[:0]
	c.intervals = c.intervals[:0]
	c.ids = c.ids[:0]

	if len(c.sets.sets) == 0 {
		return false
	}

	// 1. Compare the first series' labels between each group.
	// 2. If equal, compare the size of 2 groups.
	// NOTE(Alec), currently only compare the first lset and the size of GroupChunkSeriesSets.
	lsets, _, _ := c.sets.sets[0].At()
	c.ids = append(c.ids, 0)
	for i := 1; i < len(c.sets.sets); i++ {
		tempLabels, _, _ := c.sets.sets[i].At()
		cmp := labels.Compare(tempLabels[0], lsets[0])
		if cmp < 0{
			// Find a new lset which is the smallest.
			c.ids = c.ids[:1]
			c.ids[0] = i
			lsets = tempLabels
		} else if cmp == 0 && len(lsets) == len(tempLabels) {
			// Find a same group.
			c.ids = append(c.ids, i)
		}
	}

	// 1. Assign the gcsm->lset.
	// 2. Push group to gcsm->chunks.
	// 3. Insert gcsm->intervals.
	c.l, c.c, c.intervals = c.sets.sets[c.ids[0]].At()

	// From the second block.
	for _, i := range c.ids[1:] {
		_, tempChks, tempItvls := c.sets.sets[i].At()
		// For each series.
		for j := 0; j < len(c.c); j++ {
			c.c[j] = append(c.c[j], tempChks[j]...)
		}
		c.intervals = append(c.intervals, tempItvls...)
	}

	// Sort the group chunks of each series by min_time.
	// TODO(Alec), more efficient.
	for i := 0; i < len(c.c); i++ {
		sort.Slice(c.c[i], func(j, k int) bool { return c.c[i][j].MinTime < c.c[i][j].MinTime })
	}

	return true
}

func (c *compactionMerger) Next() bool {
	return c.nextHelper()
}

func (c *compactionMerger) Err() error {
	for _, s := range c.sets.sets {
		if s.Err() != nil {
			return s.Err()
		}
	}
	return nil
}

func (c *compactionMerger) At() ([]labels.Labels, [][]chunkenc.Meta, Intervals) {
	return c.l, c.c, c.intervals
}

// TODO(Alec), try to put these two functions to package chunks.
func MergeOverlappingGroupChunks(chks [][]chunkenc.Meta) ([][]chunkenc.Meta, error) {
	if len(chks) == 0 || len(chks[0]) < 2 {
		return chks, nil
	}
	newChks := make([][]chunkenc.Meta, 0, len(chks))
	for _, chk := range chks {
		newChks = append(newChks, []chunkenc.Meta{chk[0]})
	}
	last := 0
	for i := 1; i < len(chks[0]); i++ {
		if chks[0][i].MinTime > newChks[0][last].MaxTime {
			for j := 0; j < len(chks); j++ {
				newChks[j] = append(newChks[j], chks[j][i])
			}
			last += 1
			continue
		}
		if newChks[0][last].MaxTime < chks[0][i].MaxTime {
			for j := 0; j < len(newChks); j++ {
				newChks[j][last].MaxTime = chks[0][i].MaxTime
			}
		}

		chks1 := make([][]chunkenc.Meta, 0, len(newChks))
		chks2 := make([][]chunkenc.Meta, 0, len(newChks))
		for j := 0; j < len(newChks); j++ {
			chks1 = append(chks1, []chunkenc.Meta{newChks[j][last]})
			chks2 = append(chks2, []chunkenc.Meta{chks[j][i]})
		}
		newChk, err := MergeGroupChunks(chks1, chks2)
		if err != nil {
			return nil, err
		}

		for j := 0; j < len(newChks); j++ {
			newChks[j][last].Chunk = newChk
			newChks[j][last].SeriesRef = uint64(j)
		}
	}
	return newChks, nil
}

func MergeGroupChunks(chks1, chks2 [][]chunkenc.Meta) (chunkenc.Chunk, error) {
	// Prepare iterators from column start to end.
	it1 := newGroupChunkSeriesIterator(chks1, nil, chks1[0][0].MinTime, chks1[0][0].MaxTime)
	it2 := newGroupChunkSeriesIterator(chks2, nil, chks2[0][0].MinTime, chks2[0][0].MaxTime)

	// Create new gmc.
	gmc := chunkenc.NewGroupMemoryChunk1(len(chks1))
	app, _ := gmc.Appender()

	ok1, ok2 := it1.Next(), it2.Next()
	for ok1 && ok2 {
		t1, vals1 := it1.At()
		t2, vals2 := it2.At()
		if t1 < t2 {
			app.AppendGroup(t1, vals1)
			ok1 = it1.Next()
		} else if t1 > t2 {
			app.AppendGroup(t2, vals2)
			ok2 = it2.Next()
		} else {
			app.AppendGroup(t2, vals2)
			ok1, ok2 = it1.Next(), it2.Next()
		}
	}
	for ok1 {
		t1, vals1 := it1.At()
		app.AppendGroup(t1, vals1)
		ok1 = it1.Next()
	}
	for ok2 {
		t2, vals2 := it2.At()
		app.AppendGroup(t2, vals2)
		ok2 = it2.Next()
	}
	if it1.Err() != nil {
		return nil, it1.Err()
	}
	if it2.Err() != nil {
		return nil, it2.Err()
	}

	return gmc, nil
}
