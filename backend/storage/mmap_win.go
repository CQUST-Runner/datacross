//go:build windows

package storage

import (
	"fmt"
	"syscall"
)

// MmapFile ...
type MmapFile struct {
	h syscall.Handle
}

// Init ...
func (f *MmapFile) Init(filename string) error {
	f.h = syscall.InvalidHandle
	name, err := syscall.UTF16PtrFromString(filename)
	if err != nil {
		return err
	}
	// FILE_ATTRIBUTE_HIDDEN
	// FILE_FLAG_NO_BUFFERING
	// FILE_FLAG_SEQUENTIAL_SCAN
	// FILE_FLAG_WRITE_THROUGH
	h, err := syscall.CreateFile(name, syscall.GENERIC_READ|syscall.GENERIC_WRITE,
		syscall.FILE_SHARE_READ, nil, syscall.CREATE_ALWAYS, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return err
	}

	f.h = h
	// syscall.CreateFileMapping()
	// syscall.MapViewOfFile()
	return nil
}

// Close ...
func (f *MmapFile) Close() {
	if f.h != syscall.InvalidHandle {
		err := syscall.CloseHandle(f.h)
		if err != nil {
			fmt.Println(err)
			return
		}
		f.h = syscall.InvalidHandle
	}
}
