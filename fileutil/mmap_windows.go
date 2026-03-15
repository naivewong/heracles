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

package fileutil

import (
	"os"
	"reflect"
	"syscall"
	"unsafe"
)

func mmap(f *os.File, size int) ([]byte, error) {
	low, high := uint32(size), uint32(size>>32)
	h, errno := syscall.CreateFileMapping(syscall.Handle(f.Fd()), nil, syscall.PAGE_READONLY, high, low, nil)
	if h == 0 {
		return nil, os.NewSyscallError("CreateFileMapping", errno)
	}

	addr, errno := syscall.MapViewOfFile(h, syscall.FILE_MAP_READ, 0, 0, uintptr(size))
	if addr == 0 {
		return nil, os.NewSyscallError("MapViewOfFile", errno)
	}

	if err := syscall.CloseHandle(syscall.Handle(h)); err != nil {
		return nil, os.NewSyscallError("CloseHandle", err)
	}

	// Use reflect.SliceHeader to avoid unsafe.Pointer warning
	var sl = reflect.SliceHeader{
		Data: addr,
		Len:  size,
		Cap:  size,
	}
	return *(*[]byte)(unsafe.Pointer(&sl)), nil
}

func munmap(b []byte) error {
	// Use reflect.SliceHeader to get the data pointer safely
	var sl = (*reflect.SliceHeader)(unsafe.Pointer(&b))
	if err := syscall.UnmapViewOfFile(sl.Data); err != nil {
		return os.NewSyscallError("UnmapViewOfFile", err)
	}
	return nil
}
