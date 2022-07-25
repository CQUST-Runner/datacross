//go:build windows

package storage

import (
	"fmt"
	"io"
	"syscall"
)

type winFile struct {
	handle   syscall.Handle
	filename string
}

func (f *winFile) Close() error {
	if f.handle != syscall.InvalidHandle {
		if err := syscall.CloseHandle(f.handle); err != nil {
			fmt.Println(err)
			return err
		} else {
			f.handle = syscall.InvalidHandle
			return nil
		}
	}
	return nil
}

func (f *winFile) Read(p []byte) (n int, err error) {
	var done uint32 = 0
	err = syscall.ReadFile(f.handle, p, &done, nil)
	return int(done), err
}

// https://docs.microsoft.com/en-us/windows/win32/fileio/appending-one-file-to-another-file
func (f *winFile) Write(p []byte) (n int, err error) {
	var done uint32 = 0
	err = syscall.WriteFile(f.handle, p, &done, nil)
	return int(done), err
}

func (f *winFile) Seek(offset int64, whence int) (int64, error) {
	var base int
	switch whence {
	case io.SeekCurrent:
		base = syscall.FILE_CURRENT
	case io.SeekStart:
		base = syscall.FILE_BEGIN
	case io.SeekEnd:
		base = syscall.FILE_END
	default:
		return 0, fmt.Errorf("invalid whence")
	}
	newlowoffset, err := syscall.SetFilePointer(f.handle, int32(offset), nil, uint32(base))
	return int64(newlowoffset), err
}

func (f *winFile) Flush() error {
	return syscall.FlushFileBuffers(f.handle)
}

func (f *winFile) Path() string {
	return f.filename
}

func OpenFile(filename string, readonly bool) (File, error) {
	ptr, err := syscall.UTF16PtrFromString(filename)
	if err != nil {
		return nil, err
	}
	var access uint32 = syscall.GENERIC_READ
	if !readonly {
		access |= syscall.GENERIC_WRITE
	}
	// https://stackoverflow.com/a/11855880
	var mode uint32 = syscall.FILE_SHARE_READ
	if readonly {
		mode |= syscall.FILE_SHARE_WRITE | syscall.FILE_SHARE_DELETE
	}
	h, err := syscall.CreateFile(ptr, access, mode, nil, syscall.OPEN_ALWAYS,
		/*FILE_FLAG_SEQUENTIAL_SCAN*/
		0x08000000, 0)
	if err != nil {
		return nil, err
	}
	_, err = syscall.SetFilePointer(h, 0, nil, syscall.FILE_END)
	if err != nil {
		_ = syscall.CloseHandle(h)
		return nil, err
	}
	return &winFile{handle: h, filename: filename}, nil
}
