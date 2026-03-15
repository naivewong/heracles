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

package goversion

import (
	"testing"
)

// TestSoftwareRequiresGOVERSION1_12 verifies that the constant is properly defined.
// The constant _SoftwareRequiresGOVERSION1_12 is used as a compile-time check
// to ensure the code is built with Go 1.12 or later.
func TestSoftwareRequiresGOVERSION1_12(t *testing.T) {
	// The constant should be 0, which indicates success.
	// If the Go version is too low, compilation would fail.
	if _SoftwareRequiresGOVERSION1_12 != uint8(0) {
		t.Errorf("expected _SoftwareRequiresGOVERSION1_12 to be 0, got %d", _SoftwareRequiresGOVERSION1_12)
	}
}

// TestBuildTag verifies that the build tag is correctly applied.
// This test will only compile if the build tag "+build go1.12" is satisfied.
func TestBuildTag(t *testing.T) {
	// If this test compiles and runs, it means the Go version requirement is met.
	// The test simply verifies that the package compiled successfully.
	t.Log("Package compiled successfully with required Go version")
}
