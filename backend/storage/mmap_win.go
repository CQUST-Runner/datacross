//go:build windows

package storage

import (
	"fmt"
	"reflect"
	"syscall"
	"unsafe"
)

// MmapFile ...
type MmapFile struct {
	h    syscall.Handle
	obj  syscall.Handle
	addr uintptr
}

// Init ...
func (f *MmapFile) Init(filename string) (err error) {
	f.h = syscall.InvalidHandle
	f.obj = syscall.InvalidHandle
	f.addr = 0
	name, err := syscall.UTF16PtrFromString(filename)
	if err != nil {
		return err
	}
	// FILE_ATTRIBUTE_HIDDEN
	// FILE_FLAG_NO_BUFFERING
	// FILE_FLAG_SEQUENTIAL_SCAN
	// FILE_FLAG_WRITE_THROUGH
	h, err := syscall.CreateFile(name, syscall.GENERIC_READ|syscall.GENERIC_WRITE,
		0, nil, syscall.OPEN_ALWAYS, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			syscall.CloseHandle(h)
		}
	}()

	// high and low can be 0
	obj, err := syscall.CreateFileMapping(h, nil, syscall.PAGE_READWRITE, 0, 1024, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			syscall.CloseHandle(obj)
		}
	}()

	addr, err := syscall.MapViewOfFile(obj, syscall.FILE_MAP_WRITE, 0, 0, 0)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			syscall.UnmapViewOfFile(addr)
		}
	}()

	f.obj = obj
	f.h = h
	f.addr = addr
	// syscall.CreateFileMapping()
	// syscall.MapViewOfFile()

	// syscall.UnmapViewOfFile()
	// syscall.SetFileTime()
	return nil
}

// Close ...
func (f *MmapFile) Close() {
	if f.addr != 0 {
		err := syscall.UnmapViewOfFile(f.addr)
		if err != nil {
			fmt.Println(err)
		} else {
			f.addr = 0
		}
	}
	if f.obj != syscall.InvalidHandle {
		err := syscall.CloseHandle(f.obj)
		if err != nil {
			fmt.Println(err)
		} else {
			f.obj = syscall.InvalidHandle
		}
	}
	if f.h != syscall.InvalidHandle {
		err := syscall.CloseHandle(f.h)
		if err != nil {
			fmt.Println(err)
		} else {
			f.h = syscall.InvalidHandle
		}
	}
}

// AsSlice ...
func (f *MmapFile) AsSlice() []byte {
	sh := reflect.SliceHeader{}
	sh.Data = f.addr
	sh.Cap = 1024
	sh.Len = 1024
	return *((*[]byte)(unsafe.Pointer(&sh)))
}

// Flush ...
func (f *MmapFile) Flush() error {
	return syscall.FlushViewOfFile(f.addr, 1024)
}
