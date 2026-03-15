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
	"testing"

	"github.com/pkg/errors"
	"github.com/naivewong/tsdb-group/encoding"
	"github.com/naivewong/tsdb-group/labels"
	"github.com/naivewong/tsdb-group/testutil"
)

func TestRecord_EncodeDecode(t *testing.T) {
	var enc RecordEncoder
	var dec RecordDecoder

	series := []RefSeries{
		{
			Ref:    100,
			Labels: labels.FromStrings("abc", "def", "123", "456"),
		}, {
			Ref:    1,
			Labels: labels.FromStrings("abc", "def2", "1234", "4567"),
		}, {
			Ref:    435245,
			Labels: labels.FromStrings("xyz", "def", "foo", "bar"),
		},
	}
	decSeries, err := dec.Series(enc.Series(series, nil), nil)
	testutil.Ok(t, err)
	testutil.Equals(t, series, decSeries)

	gseries := []RefGroupSeries{
		{
			GroupRef:    0,
			Series: []RefSeries{
				RefSeries{
					Ref:    100,
					Labels: labels.FromStrings("abc", "def", "123", "456"),
				},
				RefSeries{
					Ref:    200,
					Labels: labels.FromStrings("kkp1", "ok"),
				},
			},
		}, {
			GroupRef:    198,
			Series: []RefSeries{
				RefSeries{
					Ref:    28,
					Labels: labels.FromStrings("kkp2", "ok", "1234", "4567"),
				},
				RefSeries{
					Ref:    29,
					Labels: labels.FromStrings("kkp3", "ok"),
				},
			},
		},
	}
	decGroupSeries, err := dec.GroupSeries(enc.GroupSeries(gseries, nil), nil)
	testutil.Ok(t, err)
	testutil.Equals(t, gseries, decGroupSeries)

	samples := []RefSample{
		{Ref: 0, T: 12423423, V: 1.2345},
		{Ref: 123, T: -1231, V: -123},
		{Ref: 2, T: 0, V: 99999},
	}
	decSamples, err := dec.Samples(enc.Samples(samples, nil), nil)
	testutil.Ok(t, err)
	testutil.Equals(t, samples, decSamples)

	gsamples := []RefGroupSample{
		RefGroupSample{
			GroupRef: 0,
			T:        100,
			Ids:      []uint64{19, 29, 49},
			Vals:     []float64{-2, 22.3, 33.587},
		},
		RefGroupSample{
			GroupRef: 1,
			T:        29000,
			Ids:      []uint64{4, 8, 9},
			Vals:     []float64{-138.2, 82376.3, 24.54187},
		},
		RefGroupSample{
			GroupRef: 2,
			T:        893762,
			Ids:      []uint64{3, 5, 6},
			Vals:     []float64{-22, 22.3, 33.587},
		},
	}
	decGroupSamples, err := dec.GroupSamples(enc.GroupSamples(gsamples, nil), nil)
	testutil.Ok(t, err)
	testutil.Equals(t, gsamples, decGroupSamples)

	// Intervals get split up into single entries. So we don't get back exactly
	// what we put in.
	tstones := []Stone{
		{ref: 123, intervals: Intervals{
			{Mint: -1000, Maxt: 1231231},
			{Mint: 5000, Maxt: 0},
		}},
		{ref: 13, intervals: Intervals{
			{Mint: -1000, Maxt: -11},
			{Mint: 5000, Maxt: 1000},
		}},
	}
	decTstones, err := dec.Tombstones(enc.Tombstones(tstones, nil), nil)
	testutil.Ok(t, err)
	testutil.Equals(t, []Stone{
		{ref: 123, intervals: Intervals{{Mint: -1000, Maxt: 1231231}}},
		{ref: 123, intervals: Intervals{{Mint: 5000, Maxt: 0}}},
		{ref: 13, intervals: Intervals{{Mint: -1000, Maxt: -11}}},
		{ref: 13, intervals: Intervals{{Mint: 5000, Maxt: 1000}}},
	}, decTstones)
}

// TestRecord_Corruputed ensures that corrupted records return the correct error.
// Bugfix check for pull/521 and pull/523.
func TestRecord_Corruputed(t *testing.T) {
	var enc RecordEncoder
	var dec RecordDecoder

	t.Run("Test corrupted series record", func(t *testing.T) {
		series := []RefSeries{
			{
				Ref:    100,
				Labels: labels.FromStrings("abc", "def", "123", "456"),
			},
		}

		corrupted := enc.Series(series, nil)[:8]
		_, err := dec.Series(corrupted, nil)
		testutil.Equals(t, err, encoding.ErrInvalidSize)
	})

	t.Run("Test corrupted sample record", func(t *testing.T) {
		samples := []RefSample{
			{Ref: 0, T: 12423423, V: 1.2345},
		}

		corrupted := enc.Samples(samples, nil)[:8]
		_, err := dec.Samples(corrupted, nil)
		testutil.Equals(t, errors.Cause(err), encoding.ErrInvalidSize)
	})

	t.Run("Test corrupted tombstone record", func(t *testing.T) {
		tstones := []Stone{
			{ref: 123, intervals: Intervals{
				{Mint: -1000, Maxt: 1231231},
				{Mint: 5000, Maxt: 0},
			}},
		}

		corrupted := enc.Tombstones(tstones, nil)[:8]
		_, err := dec.Tombstones(corrupted, nil)
		testutil.Equals(t, err, encoding.ErrInvalidSize)
	})
}

func TestRecord_Type(t *testing.T) {
	var dec RecordDecoder

	testutil.Equals(t, RecordInvalid, dec.Type(nil))
	testutil.Equals(t, RecordInvalid, dec.Type([]byte{}))
	testutil.Equals(t, RecordSeries, dec.Type([]byte{byte(RecordSeries)}))
	testutil.Equals(t, RecordSamples, dec.Type([]byte{byte(RecordSamples)}))
	testutil.Equals(t, RecordTombstones, dec.Type([]byte{byte(RecordTombstones)}))
	testutil.Equals(t, RecordGroupSeries, dec.Type([]byte{byte(RecordGroupSeries)}))
	testutil.Equals(t, RecordGroupSamples, dec.Type([]byte{byte(RecordGroupSamples)}))
	testutil.Equals(t, RecordGroupTombstones, dec.Type([]byte{byte(RecordGroupTombstones)}))
	testutil.Equals(t, RecordInvalid, dec.Type([]byte{255}))
}

func TestRecord_GroupSamplesEdgeCases(t *testing.T) {
	var enc RecordEncoder
	var dec RecordDecoder

	t.Run("empty samples", func(t *testing.T) {
		result := enc.GroupSamples(nil, nil)
		testutil.Equals(t, []byte(nil), result)
	})

	t.Run("single group single sample", func(t *testing.T) {
		samples := []RefGroupSample{
			{GroupRef: 1, T: 100, Ids: []uint64{10}, Vals: []float64{1.5}},
		}
		encoded := enc.GroupSamples(samples, nil)
		decoded, err := dec.GroupSamples(encoded, nil)
		testutil.Ok(t, err)
		testutil.Equals(t, samples, decoded)
	})

	t.Run("invalid record type", func(t *testing.T) {
		_, err := dec.GroupSamples([]byte{byte(RecordSeries)}, nil)
		testutil.NotOk(t, err)
	})
}

func TestRecord_GroupSeriesEdgeCases(t *testing.T) {
	var enc RecordEncoder
	var dec RecordDecoder

	t.Run("empty series", func(t *testing.T) {
		result := enc.GroupSeries(nil, nil)
		testutil.Equals(t, []byte{byte(RecordGroupSeries)}, result)
	})

	t.Run("invalid record type", func(t *testing.T) {
		_, err := dec.GroupSeries([]byte{byte(RecordSamples)}, nil)
		testutil.NotOk(t, err)
	})
}

func TestRecord_GroupTombstones(t *testing.T) {
	var enc RecordEncoder
	var dec RecordDecoder

	tstones := []Stone{
		{ref: 100, intervals: Intervals{{Mint: 0, Maxt: 100}}},
		{ref: 200, intervals: Intervals{{Mint: 50, Maxt: 150}}},
	}

	encoded := enc.GroupTombstones(tstones, nil)
	decoded, err := dec.GroupTombstones(encoded, nil)
	testutil.Ok(t, err)
	testutil.Equals(t, tstones, decoded)

	t.Run("invalid record type", func(t *testing.T) {
		_, err := dec.GroupTombstones([]byte{byte(RecordSeries)}, nil)
		testutil.NotOk(t, err)
	})
}

func TestRecord_EmptySamples(t *testing.T) {
	var enc RecordEncoder
	var dec RecordDecoder

	// Test empty samples
	encoded := enc.Samples(nil, nil)
	testutil.Equals(t, []byte{byte(RecordSamples)}, encoded)

	decoded, err := dec.Samples(encoded, nil)
	testutil.Ok(t, err)
	testutil.Equals(t, []RefSample(nil), decoded)
}
