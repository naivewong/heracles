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
	"testing"

	"github.com/naivewong/tsdb-group/testutil"
)

func TestEqualMatcher(t *testing.T) {
	m := NewEqualMatcher("foo", "bar")

	testutil.Equals(t, "foo", m.Name())
	testutil.Equals(t, true, m.Matches("bar"))
	testutil.Equals(t, false, m.Matches("foo"))
	testutil.Equals(t, false, m.Matches("foobar"))
	testutil.Equals(t, false, m.Matches(""))
	testutil.Equals(t, `foo="bar"`, m.String())
}

func TestEqualMatcherEmptyValue(t *testing.T) {
	m := NewEqualMatcher("foo", "")

	testutil.Equals(t, "foo", m.Name())
	testutil.Equals(t, true, m.Matches(""))
	testutil.Equals(t, false, m.Matches("bar"))
	testutil.Equals(t, `foo=""`, m.String())
}

func TestRegexpMatcher(t *testing.T) {
	m, err := NewRegexpMatcher("foo", "bar.*")
	testutil.Ok(t, err)

	testutil.Equals(t, "foo", m.Name())
	testutil.Equals(t, true, m.Matches("bar"))
	testutil.Equals(t, true, m.Matches("barbaz"))
	testutil.Equals(t, true, m.Matches("bar123"))
	// Note: regexp.MatchString checks if pattern matches anywhere in the string
	testutil.Equals(t, true, m.Matches("bazbar"))
	testutil.Equals(t, false, m.Matches(""))
	testutil.Equals(t, `foo=~"bar.*"`, m.String())
}

func TestRegexpMatcherFullMatch(t *testing.T) {
	m, err := NewRegexpMatcher("foo", "^bar$")
	testutil.Ok(t, err)

	testutil.Equals(t, true, m.Matches("bar"))
	testutil.Equals(t, false, m.Matches("barbaz"))
	testutil.Equals(t, false, m.Matches("foobar"))
}

func TestRegexpMatcherInvalidPattern(t *testing.T) {
	_, err := NewRegexpMatcher("foo", "[invalid")
	testutil.NotOk(t, err)
}

func TestNewMustRegexpMatcher(t *testing.T) {
	m := NewMustRegexpMatcher("foo", "bar.*")
	testutil.Equals(t, "foo", m.Name())
	testutil.Equals(t, true, m.Matches("bar"))
	testutil.Equals(t, true, m.Matches("barbaz"))
}

func TestNewMustRegexpMatcherPanic(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for invalid pattern")
		}
	}()
	NewMustRegexpMatcher("foo", "[invalid")
}

func TestNotMatcher(t *testing.T) {
	m := Not(NewEqualMatcher("foo", "bar"))

	testutil.Equals(t, false, m.Matches("bar"))
	testutil.Equals(t, true, m.Matches("foo"))
	testutil.Equals(t, true, m.Matches("foobar"))
	testutil.Equals(t, true, m.Matches(""))
	testutil.Equals(t, `not(foo="bar")`, m.String())
}

func TestNotMatcherWithRegexp(t *testing.T) {
	re, _ := NewRegexpMatcher("foo", "bar.*")
	m := Not(re)

	testutil.Equals(t, false, m.Matches("bar"))
	testutil.Equals(t, false, m.Matches("barbaz"))
	testutil.Equals(t, true, m.Matches("baz"))
	testutil.Equals(t, true, m.Matches(""))
}

func TestDoubleNotMatcher(t *testing.T) {
	m := Not(Not(NewEqualMatcher("foo", "bar")))

	testutil.Equals(t, true, m.Matches("bar"))
	testutil.Equals(t, false, m.Matches("foo"))
}

func TestSelectorMatches(t *testing.T) {
	labels := New(
		Label{"__name__", "up"},
		Label{"job", "prometheus"},
		Label{"instance", "localhost:9090"},
	)

	tests := []struct {
		name     string
		selector Selector
		expected bool
	}{
		{
			name:     "empty selector matches all",
			selector: Selector{},
			expected: true,
		},
		{
			name: "single equal matcher - match",
			selector: Selector{
				NewEqualMatcher("__name__", "up"),
			},
			expected: true,
		},
		{
			name: "single equal matcher - no match",
			selector: Selector{
				NewEqualMatcher("__name__", "down"),
			},
			expected: false,
		},
		{
			name: "multiple equal matchers - all match",
			selector: Selector{
				NewEqualMatcher("__name__", "up"),
				NewEqualMatcher("job", "prometheus"),
			},
			expected: true,
		},
		{
			name: "multiple equal matchers - one fails",
			selector: Selector{
				NewEqualMatcher("__name__", "up"),
				NewEqualMatcher("job", "node"),
			},
			expected: false,
		},
		{
			name: "regexp matcher - match",
			selector: Selector{
				NewMustRegexpMatcher("instance", "localhost:\\d+"),
			},
			expected: true,
		},
		{
			name: "regexp matcher - no match",
			selector: Selector{
				NewMustRegexpMatcher("instance", "remote.*"),
			},
			expected: false,
		},
		{
			name: "not matcher - match",
			selector: Selector{
				Not(NewEqualMatcher("job", "node")),
			},
			expected: true,
		},
		{
			name: "not matcher - no match",
			selector: Selector{
				Not(NewEqualMatcher("job", "prometheus")),
			},
			expected: false,
		},
		{
			name: "mixed matchers - all match",
			selector: Selector{
				NewEqualMatcher("__name__", "up"),
				NewMustRegexpMatcher("job", "prom.*"),
				Not(NewEqualMatcher("instance", "remote:9090")),
			},
			expected: true,
		},
		{
			name: "mixed matchers - regexp fails",
			selector: Selector{
				NewEqualMatcher("__name__", "up"),
				NewMustRegexpMatcher("job", "node.*"),
			},
			expected: false,
		},
		{
			name: "matcher on non-existent label",
			selector: Selector{
				NewEqualMatcher("nonexistent", "value"),
			},
			expected: false,
		},
		{
			name: "regexp matcher on non-existent label",
			selector: Selector{
				NewMustRegexpMatcher("nonexistent", ".*"),
			},
			// Note: .* matches empty string, so this returns true
			expected: true,
		},
		{
			name: "not matcher on non-existent label",
			selector: Selector{
				Not(NewEqualMatcher("nonexistent", "value")),
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.selector.Matches(labels)
			testutil.Equals(t, tt.expected, result)
		})
	}
}

func TestSelectorMatchesEmptyLabels(t *testing.T) {
	emptyLabels := Labels{}

	// Empty selector should match empty labels
	selector := Selector{}
	testutil.Equals(t, true, selector.Matches(emptyLabels))

	// Non-empty selector should not match empty labels
	selector = Selector{NewEqualMatcher("foo", "bar")}
	testutil.Equals(t, false, selector.Matches(emptyLabels))
}

func TestSelectorMatchesWithEmptyValue(t *testing.T) {
	labels := New(
		Label{"foo", ""},
		Label{"bar", "baz"},
	)

	// Matcher for empty value
	selector := Selector{NewEqualMatcher("foo", "")}
	testutil.Equals(t, true, selector.Matches(labels))

	// Not matcher for empty value
	selector = Selector{Not(NewEqualMatcher("foo", "bar"))}
	testutil.Equals(t, true, selector.Matches(labels))
}

func TestRegexpMatcherSpecialPatterns(t *testing.T) {
	tests := []struct {
		pattern string
		input   string
		match   bool
	}{
		{".*", "anything", true},
		{".*", "", true},
		{"^$", "", true},
		{"^$", "notempty", false},
		{"^foo$", "foo", true},
		{"^foo$", "foobar", false},
		{"foo|bar", "foo", true},
		{"foo|bar", "bar", true},
		{"foo|bar", "baz", false},
		{"\\d+", "123", true},
		{"\\d+", "abc", false},
	}

	for _, tt := range tests {
		m, err := NewRegexpMatcher("test", tt.pattern)
		testutil.Ok(t, err)
		result := m.Matches(tt.input)
		testutil.Equals(t, tt.match, result, "pattern=%q, input=%q", tt.pattern, tt.input)
	}
}

func TestMatcherInterfaceImplementations(t *testing.T) {
	// Verify all matcher types implement the Matcher interface
	var _ Matcher = NewEqualMatcher("foo", "bar")

	re, _ := NewRegexpMatcher("foo", "bar")
	var _ Matcher = re

	var _ Matcher = Not(NewEqualMatcher("foo", "bar"))
}

func BenchmarkEqualMatcherMatches(b *testing.B) {
	m := NewEqualMatcher("foo", "bar")
	labels := New(Label{"foo", "bar"})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		m.Matches(labels.Get("foo"))
	}
}

func BenchmarkRegexpMatcherMatches(b *testing.B) {
	m := NewMustRegexpMatcher("foo", "bar.*")
	labels := New(Label{"foo", "barbaz"})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		m.Matches(labels.Get("foo"))
	}
}

func BenchmarkSelectorMatches(b *testing.B) {
	labels := New(
		Label{"__name__", "up"},
		Label{"job", "prometheus"},
		Label{"instance", "localhost:9090"},
	)

	selector := Selector{
		NewEqualMatcher("__name__", "up"),
		NewEqualMatcher("job", "prometheus"),
		NewMustRegexpMatcher("instance", "localhost:\\d+"),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		selector.Matches(labels)
	}
}
