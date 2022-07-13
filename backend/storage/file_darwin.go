//go:build darwin

package storage

import (
	"syscall"
)

type darwinFlie struct {
	handle   int
	filename string
}

func (f *darwinFlie) Close() error {
	if f.handle != -1 {
		if err := syscall.Close(f.handle); err != nil {
			return err
		}
		f.handle = -1
	}
	return nil
}

func (f *darwinFlie) Read(p []byte) (n int, err error) {
	return syscall.Read(f.handle, p)
}

func (f *darwinFlie) Write(p []byte) (n int, err error) {
	return syscall.Write(f.handle, p)
}

func (f *darwinFlie) Seek(offset int64, whence int) (int64, error) {
	return syscall.Seek(f.handle, offset, whence)
}

func (f *darwinFlie) Flush() error {
	return syscall.Fsync(f.handle)
}

func (f *darwinFlie) Path() string {
	return f.filename
}

func OpenFile(filename string, readonly bool) (File, error) {
	var access int
	if readonly {
		access = syscall.O_RDONLY
	} else {
		access = syscall.O_RDWR
	}
	fd, err := syscall.Open(filename, syscall.O_CLOEXEC|syscall.O_CREAT|access, 0666)
	if err != nil {
		return nil, err
	}
	return &darwinFlie{handle: fd, filename: filename}, nil
}
