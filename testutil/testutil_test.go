// The MIT License (MIT)

// Copyright (c) 2014 Ben Johnson

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

package testutil

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestAssert(t *testing.T) {
	// Test passing condition
	Assert(t, true, "should not fail")
	
	// Test with format arguments
	Assert(t, true, "message with %s", "args")
}

func TestOk(t *testing.T) {
	// Test with nil error
	Ok(t, nil)
}

func TestNotOk(t *testing.T) {
	// Test with error
	NotOk(t, errors.New("test error"))
}

func TestEquals(t *testing.T) {
	// Test equal values
	Equals(t, 1, 1)
	Equals(t, "hello", "hello")
	Equals(t, []int{1, 2, 3}, []int{1, 2, 3})
	
	// Test with message
	Equals(t, 1, 1, "values should be equal")
	Equals(t, 1, 1, "values %d and %d should be equal", 1, 1)
}

func TestNotEquals(t *testing.T) {
	// Test not equal values
	NotEquals(t, 1, 2)
	NotEquals(t, "hello", "world")
}

func TestAssert2(t *testing.T) {
	// Test passing condition
	Assert2(true, "should not fail")
	Assert2(true, "message with %s", "args")
}

func TestOk2(t *testing.T) {
	// Test with nil error - should not panic
	defer func() {
		if r := recover(); r != nil {
			t.Error("Ok2 should not panic with nil error")
		}
	}()
	Ok2(nil)
}

func TestNotOk2(t *testing.T) {
	// Test with error - should not panic
	defer func() {
		if r := recover(); r != nil {
			t.Error("NotOk2 should not panic with error")
		}
	}()
	NotOk2(errors.New("test error"))
}

func TestEquals2(t *testing.T) {
	// Test equal values - should not panic
	defer func() {
		if r := recover(); r != nil {
			t.Error("Equals2 should not panic with equal values")
		}
	}()
	Equals2(1, 1)
	Equals2("hello", "hello")
	Equals2(1, 1, "values should be equal")
}

func TestNotEquals2(t *testing.T) {
	// Test not equal values - should not panic
	defer func() {
		if r := recover(); r != nil {
			t.Error("NotEquals2 should not panic with different values")
		}
	}()
	NotEquals2(1, 2)
	NotEquals2("hello", "world")
}

func TestFormatMessage(t *testing.T) {
	// Test empty args
	result := formatMessage(nil)
	if result != "" {
		t.Errorf("expected empty string, got %q", result)
	}
	
	// Test with string message
	result = formatMessage([]interface{}{"test message"})
	if result != "\n\nmsg: test message" {
		t.Errorf("unexpected result: %q", result)
	}
	
	// Test with format arguments
	result = formatMessage([]interface{}{"value: %d", 42})
	if result != "\n\nmsg: value: 42" {
		t.Errorf("unexpected result: %q", result)
	}
	
	// Test with non-string first argument
	result = formatMessage([]interface{}{42})
	if result != "" {
		t.Errorf("expected empty string for non-string first arg, got %q", result)
	}
}

func TestRemoveAll(t *testing.T) {
	// Test removing non-existent directory (should succeed)
	err := RemoveAll(filepath.Join(os.TempDir(), "nonexistent-dir-12345"))
	if err != nil {
		// On some systems, RemoveAll might return an error for non-existent paths
		// But os.RemoveAll actually returns nil for non-existent paths
		t.Logf("RemoveAll on non-existent dir returned: %v", err)
	}
	
	// Test removing existing directory
	tmpDir, err := ioutil.TempDir("", "testutil-removeall")
	if err != nil {
		t.Fatal(err)
	}
	
	// Create some files inside
	testFile := filepath.Join(tmpDir, "test.txt")
	if err := ioutil.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}
	
	// Remove the directory
	err = RemoveAll(tmpDir)
	if err != nil {
		t.Errorf("RemoveAll failed: %v", err)
	}
	
	// Verify it's gone
	if _, err := os.Stat(tmpDir); !os.IsNotExist(err) {
		t.Errorf("directory should be removed")
	}
}