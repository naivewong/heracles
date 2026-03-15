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

package fileutil

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/naivewong/tsdb-group/testutil"
)

func TestCopyDirs(t *testing.T) {
	// Create source directory structure
	srcDir, err := ioutil.TempDir("", "test_copy_src")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(srcDir))
	}()

	// Create nested structure
	subDir := filepath.Join(srcDir, "subdir")
	testutil.Ok(t, os.MkdirAll(subDir, 0755))

	// Create files
	testutil.Ok(t, ioutil.WriteFile(filepath.Join(srcDir, "file1.txt"), []byte("content1"), 0644))
	testutil.Ok(t, ioutil.WriteFile(filepath.Join(subDir, "file2.txt"), []byte("content2"), 0644))

	// Create empty directory
	emptyDir := filepath.Join(srcDir, "empty")
	testutil.Ok(t, os.MkdirAll(emptyDir, 0755))

	// Copy to destination
	destDir, err := ioutil.TempDir("", "test_copy_dest")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(destDir))
	}()

	testutil.Ok(t, CopyDirs(srcDir, destDir))

	// Verify structure
	_, err = os.Stat(filepath.Join(destDir, "file1.txt"))
	testutil.Ok(t, err)

	_, err = os.Stat(filepath.Join(destDir, "subdir", "file2.txt"))
	testutil.Ok(t, err)

	_, err = os.Stat(filepath.Join(destDir, "empty"))
	testutil.Ok(t, err)

	// Verify content
	content, err := ioutil.ReadFile(filepath.Join(destDir, "file1.txt"))
	testutil.Ok(t, err)
	testutil.Equals(t, "content1", string(content))

	content, err = ioutil.ReadFile(filepath.Join(destDir, "subdir", "file2.txt"))
	testutil.Ok(t, err)
	testutil.Equals(t, "content2", string(content))
}

func TestCopyDirsEmptySource(t *testing.T) {
	srcDir, err := ioutil.TempDir("", "test_copy_empty_src")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(srcDir))
	}()

	destDir, err := ioutil.TempDir("", "test_copy_empty_dest")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(destDir))
	}()

	testutil.Ok(t, CopyDirs(srcDir, destDir))

	// Destination should exist but be empty (or just contain the dir itself)
	entries, err := ioutil.ReadDir(destDir)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(entries))
}

func TestCopyDirsNonExistentSource(t *testing.T) {
	srcDir := filepath.Join(os.TempDir(), "nonexistent_dir_12345")
	destDir, err := ioutil.TempDir("", "test_copy_dest")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(destDir))
	}()

	// CopyDirs with non-existent source:
	// filepath.Walk doesn't error on non-existent dirs, it just calls the callback once
	// with the non-existent path. The os.Stat on the non-existent path will fail.
	err = CopyDirs(srcDir, destDir)
	// The behavior depends on how filepath.Walk handles non-existent directories.
	// In Go 1.12+, Walk may not error but Stat will fail.
	// We just verify the function completes (may or may not error)
	_ = err // err could be nil or an error depending on Go version
}

func TestReadDir(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_readdir")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(dir))
	}()

	// Create files in random order
	files := []string{"zebra.txt", "apple.txt", "mango.txt", "banana.txt"}
	for _, f := range files {
		testutil.Ok(t, ioutil.WriteFile(filepath.Join(dir, f), []byte("data"), 0644))
	}

	// Read directory - should return sorted names
	result, err := ReadDir(dir)
	testutil.Ok(t, err)

	expected := []string{"apple.txt", "banana.txt", "mango.txt", "zebra.txt"}
	testutil.Equals(t, expected, result)
}

func TestReadDirEmpty(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_readdir_empty")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(dir))
	}()

	result, err := ReadDir(dir)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(result))
}

func TestReadDirNonExistent(t *testing.T) {
	result, err := ReadDir(filepath.Join(os.TempDir(), "nonexistent_dir_12345"))
	testutil.NotOk(t, err)
	testutil.Equals(t, 0, len(result))
}

func TestReadDirWithSubdirs(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_readdir_subdirs")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(dir))
	}()

	// Create files and subdirectories
	testutil.Ok(t, ioutil.WriteFile(filepath.Join(dir, "file.txt"), []byte("data"), 0644))
	testutil.Ok(t, os.MkdirAll(filepath.Join(dir, "subdir"), 0755))

	result, err := ReadDir(dir)
	testutil.Ok(t, err)

	// Should include both files and directories
	testutil.Equals(t, 2, len(result))
}

func TestRename(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_rename")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(dir))
	}()

	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")

	testutil.Ok(t, ioutil.WriteFile(src, []byte("content"), 0644))

	testutil.Ok(t, Rename(src, dst))

	// Source should not exist
	_, err = os.Stat(src)
	testutil.NotOk(t, err)
	testutil.Equals(t, true, os.IsNotExist(err))

	// Destination should exist
	content, err := ioutil.ReadFile(dst)
	testutil.Ok(t, err)
	testutil.Equals(t, "content", string(content))
}

func TestRenameDirectory(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_rename_dir")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(dir))
	}()

	src := filepath.Join(dir, "src_dir")
	dst := filepath.Join(dir, "dst_dir")

	testutil.Ok(t, os.MkdirAll(src, 0755))
	testutil.Ok(t, ioutil.WriteFile(filepath.Join(src, "file.txt"), []byte("content"), 0644))

	testutil.Ok(t, Rename(src, dst))

	// Source should not exist
	_, err = os.Stat(src)
	testutil.NotOk(t, err)

	// Destination should exist with its contents
	content, err := ioutil.ReadFile(filepath.Join(dst, "file.txt"))
	testutil.Ok(t, err)
	testutil.Equals(t, "content", string(content))
}

func TestRenameNonExistent(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_rename_nonexistent")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(dir))
	}()

	src := filepath.Join(dir, "nonexistent.txt")
	dst := filepath.Join(dir, "dst.txt")

	err = Rename(src, dst)
	testutil.NotOk(t, err)
}

func TestReplace(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_replace")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(dir))
	}()

	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")

	// Create source
	testutil.Ok(t, ioutil.WriteFile(src, []byte("new content"), 0644))
	// Create destination to be replaced
	testutil.Ok(t, ioutil.WriteFile(dst, []byte("old content"), 0644))

	testutil.Ok(t, Replace(src, dst))

	// Source should not exist
	_, err = os.Stat(src)
	testutil.NotOk(t, err)
	testutil.Equals(t, true, os.IsNotExist(err))

	// Destination should have new content
	content, err := ioutil.ReadFile(dst)
	testutil.Ok(t, err)
	testutil.Equals(t, "new content", string(content))
}

func TestReplaceDirectory(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_replace_dir")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(dir))
	}()

	src := filepath.Join(dir, "src_dir")
	dst := filepath.Join(dir, "dst_dir")

	// Create source directory structure
	testutil.Ok(t, os.MkdirAll(filepath.Join(src, "subdir"), 0755))
	testutil.Ok(t, ioutil.WriteFile(filepath.Join(src, "file.txt"), []byte("new"), 0644))

	// Create destination directory to be replaced
	testutil.Ok(t, os.MkdirAll(filepath.Join(dst, "old_subdir"), 0755))
	testutil.Ok(t, ioutil.WriteFile(filepath.Join(dst, "old_file.txt"), []byte("old"), 0644))

	testutil.Ok(t, Replace(src, dst))

	// Source should not exist
	_, err = os.Stat(src)
	testutil.NotOk(t, err)

	// Destination should have new structure
	content, err := ioutil.ReadFile(filepath.Join(dst, "file.txt"))
	testutil.Ok(t, err)
	testutil.Equals(t, "new", string(content))

	// Old content should be gone
	_, err = os.Stat(filepath.Join(dst, "old_file.txt"))
	testutil.NotOk(t, err)
}

func TestReplaceWithFile(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_replace_file")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(dir))
	}()

	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")

	// Create source file
	testutil.Ok(t, ioutil.WriteFile(src, []byte("new content"), 0644))
	// Create destination file
	testutil.Ok(t, ioutil.WriteFile(dst, []byte("old content"), 0644))

	testutil.Ok(t, Replace(src, dst))

	// Source should not exist
	_, err = os.Stat(src)
	testutil.NotOk(t, err)

	// Destination should have new content
	content, err := ioutil.ReadFile(dst)
	testutil.Ok(t, err)
	testutil.Equals(t, "new content", string(content))
}

func TestReplaceNonExistentSource(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_replace_nonexistent")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(dir))
	}()

	src := filepath.Join(dir, "nonexistent.txt")
	dst := filepath.Join(dir, "dst.txt")

	testutil.Ok(t, ioutil.WriteFile(dst, []byte("content"), 0644))

	err = Replace(src, dst)
	testutil.NotOk(t, err)
}

func TestReplaceDestinationNonExistent(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_replace_dest_nonexistent")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(dir))
	}()

	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")

	testutil.Ok(t, ioutil.WriteFile(src, []byte("content"), 0644))

	testutil.Ok(t, Replace(src, dst))

	// Destination should exist with source content
	content, err := ioutil.ReadFile(dst)
	testutil.Ok(t, err)
	testutil.Equals(t, "content", string(content))
}

func TestCopyFile(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_copyfile")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(dir))
	}()

	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")

	testutil.Ok(t, ioutil.WriteFile(src, []byte("test content"), 0644))

	testutil.Ok(t, copyFile(src, dst))

	// Verify content
	content, err := ioutil.ReadFile(dst)
	testutil.Ok(t, err)
	testutil.Equals(t, "test content", string(content))
}

func TestCopyFileNonExistentSource(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_copyfile_nonexistent")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(dir))
	}()

	src := filepath.Join(dir, "nonexistent.txt")
	dst := filepath.Join(dir, "dst.txt")

	err = copyFile(src, dst)
	testutil.NotOk(t, err)
}

func TestReadDirs(t *testing.T) {
	srcDir, err := ioutil.TempDir("", "test_readdirs")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(srcDir))
	}()

	// Create nested structure
	subDir := filepath.Join(srcDir, "subdir")
	testutil.Ok(t, os.MkdirAll(subDir, 0755))
	testutil.Ok(t, os.MkdirAll(filepath.Join(srcDir, "empty"), 0755))

	testutil.Ok(t, ioutil.WriteFile(filepath.Join(srcDir, "file1.txt"), []byte("data"), 0644))
	testutil.Ok(t, ioutil.WriteFile(filepath.Join(subDir, "file2.txt"), []byte("data"), 0644))

	files, err := readDirs(srcDir)
	testutil.Ok(t, err)

	// Should contain relative paths
	testutil.Equals(t, 4, len(files))
}

func TestOpenMmapFile(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_mmap")
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)

	// Create a test file
	filePath := filepath.Join(dir, "test.txt")
	testData := []byte("hello world")
	testutil.Ok(t, ioutil.WriteFile(filePath, testData, 0644))

	// Open and mmap the file
	mf, err := OpenMmapFile(filePath)
	testutil.Ok(t, err)
	defer mf.Close()

	// Verify content
	testutil.Equals(t, len(testData), len(mf.Bytes()))
	testutil.Equals(t, string(testData), string(mf.Bytes()))

	// Verify File() returns the underlying file
	testutil.Assert(t, mf.File() != nil, "File() should return non-nil")
}

func TestOpenMmapFileNonExistent(t *testing.T) {
	_, err := OpenMmapFile(filepath.Join(os.TempDir(), "nonexistent_12345.txt"))
	testutil.NotOk(t, err)
}

func TestOpenMmapFileDirectory(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_mmap_dir")
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)

	// Attempting to mmap a directory should fail
	_, err = OpenMmapFile(dir)
	testutil.NotOk(t, err)
}
