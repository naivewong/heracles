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

package labels

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"

	"github.com/naivewong/tsdb-group/testutil"
)

func TestCompareAndEquals(t *testing.T) {
	cases := []struct {
		a, b []Label
		res  int
	}{
		{
			a:   []Label{},
			b:   []Label{},
			res: 0,
		},
		{
			a:   []Label{{"a", ""}},
			b:   []Label{{"a", ""}, {"b", ""}},
			res: -1,
		},
		{
			a:   []Label{{"a", ""}},
			b:   []Label{{"a", ""}},
			res: 0,
		},
		{
			a:   []Label{{"aa", ""}, {"aa", ""}},
			b:   []Label{{"aa", ""}, {"ab", ""}},
			res: -1,
		},
		{
			a:   []Label{{"aa", ""}, {"abb", ""}},
			b:   []Label{{"aa", ""}, {"ab", ""}},
			res: 1,
		},
		{
			a: []Label{
				{"__name__", "go_gc_duration_seconds"},
				{"job", "prometheus"},
				{"quantile", "0.75"},
			},
			b: []Label{
				{"__name__", "go_gc_duration_seconds"},
				{"job", "prometheus"},
				{"quantile", "1"},
			},
			res: -1,
		},
		{
			a: []Label{
				{"handler", "prometheus"},
				{"instance", "localhost:9090"},
			},
			b: []Label{
				{"handler", "query"},
				{"instance", "localhost:9090"},
			},
			res: -1,
		},
	}
	for _, c := range cases {
		// Use constructor to ensure sortedness.
		a, b := New(c.a...), New(c.b...)

		testutil.Equals(t, c.res, Compare(a, b))
		testutil.Equals(t, c.res == 0, a.Equals(b))
	}
}

func BenchmarkSliceSort(b *testing.B) {
	lbls, err := ReadLabels(filepath.Join("..", "testdata", "20kseries.json"), 20000)
	testutil.Ok(b, err)

	for len(lbls) < 20e6 {
		lbls = append(lbls, lbls...)
	}
	for i := range lbls {
		j := rand.Intn(i + 1)
		lbls[i], lbls[j] = lbls[j], lbls[i]
	}

	for _, k := range []int{
		100, 5000, 50000, 300000, 900000, 5e6, 20e6,
	} {
		b.Run(fmt.Sprintf("%d", k), func(b *testing.B) {
			b.ReportAllocs()

			for a := 0; a < b.N; a++ {
				b.StopTimer()
				cl := make(Slice, k)
				copy(cl, Slice(lbls[:k]))
				b.StartTimer()

				sort.Sort(cl)
			}
		})
	}
}

func BenchmarkLabelSetFromMap(b *testing.B) {
	m := map[string]string{
		"job":       "node",
		"instance":  "123.123.1.211:9090",
		"path":      "/api/v1/namespaces/<namespace>/deployments/<name>",
		"method":    "GET",
		"namespace": "system",
		"status":    "500",
	}
	var ls Labels
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ls = FromMap(m)
	}
	_ = ls
}

func BenchmarkMapFromLabels(b *testing.B) {
	m := map[string]string{
		"job":       "node",
		"instance":  "123.123.1.211:9090",
		"path":      "/api/v1/namespaces/<namespace>/deployments/<name>",
		"method":    "GET",
		"namespace": "system",
		"status":    "500",
	}
	ls := FromMap(m)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = ls.Map()
	}
}

func BenchmarkLabelSetEquals(b *testing.B) {
	// The vast majority of comparisons will be against a matching label set.
	m := map[string]string{
		"job":       "node",
		"instance":  "123.123.1.211:9090",
		"path":      "/api/v1/namespaces/<namespace>/deployments/<name>",
		"method":    "GET",
		"namespace": "system",
		"status":    "500",
	}
	ls := FromMap(m)
	var res bool

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		res = ls.Equals(ls)
	}
	_ = res
}

func BenchmarkLabelSetHash(b *testing.B) {
	// The vast majority of comparisons will be against a matching label set.
	m := map[string]string{
		"job":       "node",
		"instance":  "123.123.1.211:9090",
		"path":      "/api/v1/namespaces/<namespace>/deployments/<name>",
		"method":    "GET",
		"namespace": "system",
		"status":    "500",
	}
	ls := FromMap(m)
	var res uint64

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		res += ls.Hash()
	}
	fmt.Println(res)
}

func TestByGroup(t *testing.T) {
	lsets := []Labels{
		{{"c", "1"}},
		{{"d", "1"}},
		{{"a", "1"}},
		{{"b", "1"}},
		{{"x", "1"}},
		{{"y", "1"}},
	}
	g := &ByGroup{Lsets: &lsets, GSize: 2}
	sort.Sort(g)
	t.Log(lsets)
}

func TestLabelsHash(t *testing.T) {
	ls := Labels{{"a", "1"}, {"b", "2"}}
	hash := ls.Hash()
	testutil.Assert(t, hash != 0, "hash should not be zero")

	// Same labels should produce same hash
	ls2 := Labels{{"a", "1"}, {"b", "2"}}
	testutil.Equals(t, hash, ls2.Hash())

	// Different labels should produce different hash
	ls3 := Labels{{"a", "1"}, {"b", "3"}}
	testutil.Assert(t, hash != ls3.Hash(), "different labels should have different hash")
}

func TestLabelsMap(t *testing.T) {
	ls := Labels{{"a", "1"}, {"b", "2"}}
	m := ls.Map()

	testutil.Equals(t, 2, len(m))
	testutil.Equals(t, "1", m["a"])
	testutil.Equals(t, "2", m["b"])
}

func TestLabelsWithoutEmpty(t *testing.T) {
	// No empty labels
	ls := Labels{{"a", "1"}, {"b", "2"}}
	result := ls.WithoutEmpty()
	testutil.Equals(t, 2, len(result))

	// With empty label
	ls2 := Labels{{"a", "1"}, {"b", ""}}
	result2 := ls2.WithoutEmpty()
	testutil.Equals(t, 1, len(result2))
	testutil.Equals(t, "1", result2[0].Value)
}

func TestFromMap(t *testing.T) {
	m := map[string]string{
		"a": "1",
		"b": "2",
		"c": "", // Empty value should be excluded
	}
	ls := FromMap(m)

	testutil.Equals(t, 2, len(ls))
}

func TestFromStrings(t *testing.T) {
	ls := FromStrings("a", "1", "b", "2")
	testutil.Equals(t, 2, len(ls))

	// Empty value should be excluded
	ls2 := FromStrings("a", "1", "b", "")
	testutil.Equals(t, 1, len(ls2))
}

func TestFromStringsPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for odd number of strings")
		}
	}()
	FromStrings("a") // Should panic
}

func TestSliceMethods(t *testing.T) {
	s := Slice{
		{{"b", "2"}},
		{{"a", "1"}},
	}

	testutil.Equals(t, 2, s.Len())
	testutil.Assert(t, s.Less(1, 0), "a should be less than b")

	s.Swap(0, 1)
	testutil.Equals(t, "a", s[0][0].Name)
}

func TestMatcherValue(t *testing.T) {
	// Test EqualMatcher.Value()
	em := &EqualMatcher{name: "name", value: "value"}
	testutil.Equals(t, "value", em.Value())

	// Test RegexpMatcher.Value()
	rm, err := NewRegexpMatcher("name", "v.*")
	testutil.Ok(t, err)
	// Type assertion to access Value()
	if rm, ok := rm.(*RegexpMatcher); ok {
		testutil.Equals(t, "v.*", rm.Value())
	} else {
		t.Error("expected *RegexpMatcher")
	}
}

func TestByGroupAt(t *testing.T) {
	lsets := []Labels{
		{{"a", "1"}},
		{{"b", "2"}},
		{{"c", "3"}},
		{{"d", "4"}},
	}
	g := &ByGroup{Lsets: &lsets, GSize: 2}

	group, err := g.At(0)
	testutil.Ok(t, err)
	testutil.Equals(t, 2, len(group))

	group2, err := g.At(2)
	testutil.Ok(t, err)
	testutil.Equals(t, 2, len(group2))
}
