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

package errors

import (
	"errors"
	"fmt"
	"testing"

	"github.com/naivewong/tsdb-group/testutil"
)

// ============== Tests for MultiError.Error ==============

func TestMultiErrorError(t *testing.T) {
	t.Run("empty multi error", func(t *testing.T) {
		merr := MultiError{}
		testutil.Equals(t, "", merr.Error())
	})

	t.Run("single error", func(t *testing.T) {
		merr := MultiError{errors.New("error1")}
		testutil.Equals(t, "error1", merr.Error())
	})

	t.Run("two errors", func(t *testing.T) {
		merr := MultiError{errors.New("error1"), errors.New("error2")}
		testutil.Equals(t, "2 errors: error1; error2", merr.Error())
	})

	t.Run("three errors", func(t *testing.T) {
		merr := MultiError{errors.New("error1"), errors.New("error2"), errors.New("error3")}
		testutil.Equals(t, "3 errors: error1; error2; error3", merr.Error())
	})

	t.Run("many errors", func(t *testing.T) {
		merr := MultiError{
			errors.New("error1"),
			errors.New("error2"),
			errors.New("error3"),
			errors.New("error4"),
			errors.New("error5"),
		}
		testutil.Equals(t, "5 errors: error1; error2; error3; error4; error5", merr.Error())
	})

	t.Run("error with empty message", func(t *testing.T) {
		merr := MultiError{errors.New("")}
		testutil.Equals(t, "", merr.Error())
	})

	t.Run("nested multi error", func(t *testing.T) {
		inner := MultiError{errors.New("inner1"), errors.New("inner2")}
		outer := MultiError{errors.New("outer1")}
		outer.Add(inner)
		// Nested MultiError is flattened when using Add
		testutil.Equals(t, "3 errors: outer1; inner1; inner2", outer.Error())
	})
}

// ============== Tests for MultiError.Add ==============

func TestMultiErrorAdd(t *testing.T) {
	t.Run("add nil error", func(t *testing.T) {
		var merr MultiError
		merr.Add(nil)
		testutil.Equals(t, 0, len(merr))
	})

	t.Run("add single error", func(t *testing.T) {
		var merr MultiError
		merr.Add(errors.New("error1"))
		testutil.Equals(t, 1, len(merr))
		testutil.Equals(t, "error1", merr[0].Error())
	})

	t.Run("add multiple errors", func(t *testing.T) {
		var merr MultiError
		merr.Add(errors.New("error1"))
		merr.Add(errors.New("error2"))
		merr.Add(errors.New("error3"))
		testutil.Equals(t, 3, len(merr))
		testutil.Equals(t, "error1", merr[0].Error())
		testutil.Equals(t, "error2", merr[1].Error())
		testutil.Equals(t, "error3", merr[2].Error())
	})

	t.Run("add nil among errors", func(t *testing.T) {
		var merr MultiError
		merr.Add(errors.New("error1"))
		merr.Add(nil)
		merr.Add(errors.New("error2"))
		merr.Add(nil)
		testutil.Equals(t, 2, len(merr))
	})

	t.Run("add another MultiError", func(t *testing.T) {
		var merr MultiError
		inner := MultiError{errors.New("inner1"), errors.New("inner2")}
		merr.Add(inner)
		testutil.Equals(t, 2, len(merr))
		testutil.Equals(t, "inner1", merr[0].Error())
		testutil.Equals(t, "inner2", merr[1].Error())
	})

	t.Run("add multiple MultiErrors", func(t *testing.T) {
		var merr MultiError
		merr.Add(MultiError{errors.New("a1"), errors.New("a2")})
		merr.Add(MultiError{errors.New("b1"), errors.New("b2")})
		testutil.Equals(t, 4, len(merr))
		testutil.Equals(t, "a1", merr[0].Error())
		testutil.Equals(t, "a2", merr[1].Error())
		testutil.Equals(t, "b1", merr[2].Error())
		testutil.Equals(t, "b2", merr[3].Error())
	})

	t.Run("add nil and MultiError", func(t *testing.T) {
		var merr MultiError
		merr.Add(nil)
		merr.Add(MultiError{errors.New("error1")})
		merr.Add(nil)
		testutil.Equals(t, 1, len(merr))
	})

	t.Run("mix of errors and MultiErrors", func(t *testing.T) {
		var merr MultiError
		merr.Add(errors.New("e1"))
		merr.Add(MultiError{errors.New("m1"), errors.New("m2")})
		merr.Add(errors.New("e2"))
		merr.Add(nil)
		testutil.Equals(t, 4, len(merr))
		testutil.Equals(t, "e1", merr[0].Error())
		testutil.Equals(t, "m1", merr[1].Error())
		testutil.Equals(t, "m2", merr[2].Error())
		testutil.Equals(t, "e2", merr[3].Error())
	})
}

// ============== Tests for MultiError.Err ==============

func TestMultiErrorErr(t *testing.T) {
	t.Run("empty multi error returns nil", func(t *testing.T) {
		merr := MultiError{}
		testutil.Equals(t, error(nil), merr.Err())
	})

	t.Run("single error returns self", func(t *testing.T) {
		merr := MultiError{errors.New("error1")}
		err := merr.Err()
		testutil.NotOk(t, err)
		testutil.Equals(t, "error1", err.Error())
		// Should return the same MultiError
		testutil.Equals(t, merr, err)
	})

	t.Run("multiple errors returns self", func(t *testing.T) {
		merr := MultiError{errors.New("error1"), errors.New("error2")}
		err := merr.Err()
		testutil.NotOk(t, err)
		testutil.Equals(t, "2 errors: error1; error2", err.Error())
		testutil.Equals(t, merr, err)
	})

	t.Run("after adding nil", func(t *testing.T) {
		var merr MultiError
		merr.Add(nil)
		testutil.Equals(t, error(nil), merr.Err())
	})

	t.Run("after adding error", func(t *testing.T) {
		var merr MultiError
		merr.Add(errors.New("error1"))
		err := merr.Err()
		testutil.NotOk(t, err)
		testutil.Equals(t, "error1", err.Error())
	})
}

// ============== Integration Tests ==============

func TestMultiErrorUsage(t *testing.T) {
	t.Run("typical usage pattern", func(t *testing.T) {
		var merr MultiError

		// Simulate collecting errors from multiple operations
		for i := 0; i < 5; i++ {
			if i%2 == 0 {
				merr.Add(fmt.Errorf("error %d", i))
			} else {
				// Simulate successful operation (no error)
				merr.Add(nil)
			}
		}

		// Should have 3 errors (0, 2, 4)
		testutil.Equals(t, 3, len(merr))

		// Err() should return non-nil
		testutil.NotOk(t, merr.Err())

		// Error string should mention count
		errStr := merr.Error()
		testutil.Equals(t, "3 errors: error 0; error 2; error 4", errStr)
	})

	t.Run("all operations succeed", func(t *testing.T) {
		var merr MultiError

		for i := 0; i < 5; i++ {
			merr.Add(nil)
		}

		testutil.Equals(t, 0, len(merr))
		testutil.Equals(t, error(nil), merr.Err())
	})

	t.Run("all operations fail", func(t *testing.T) {
		var merr MultiError

		for i := 0; i < 3; i++ {
			merr.Add(fmt.Errorf("operation %d failed", i))
		}

		testutil.Equals(t, 3, len(merr))
		testutil.NotOk(t, merr.Err())
	})
}

func TestMultiErrorAsError(t *testing.T) {
	// MultiError implements error interface
	var _ error = MultiError{}

	// Can be used where error is expected
	var err error
	err = MultiError{errors.New("test")}
	testutil.NotOk(t, err)
	testutil.Equals(t, "test", err.Error())
}

// ============== Benchmarks ==============

func BenchmarkMultiErrorAdd(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var merr MultiError
		for j := 0; j < 10; j++ {
			merr.Add(fmt.Errorf("error %d", j))
		}
	}
}

func BenchmarkMultiErrorAddWithNil(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var merr MultiError
		for j := 0; j < 10; j++ {
			if j%2 == 0 {
				merr.Add(fmt.Errorf("error %d", j))
			} else {
				merr.Add(nil)
			}
		}
	}
}

func BenchmarkMultiErrorError(b *testing.B) {
	merr := MultiError{
		errors.New("error1"),
		errors.New("error2"),
		errors.New("error3"),
		errors.New("error4"),
		errors.New("error5"),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = merr.Error()
	}
}

func BenchmarkMultiErrorErr(b *testing.B) {
	merr := MultiError{
		errors.New("error1"),
		errors.New("error2"),
		errors.New("error3"),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = merr.Err()
	}
}

func BenchmarkMultiErrorEmpty(b *testing.B) {
	var merr MultiError

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = merr.Err()
	}
}
