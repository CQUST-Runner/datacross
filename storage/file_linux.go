//go:build linux

package storage

import (
	"syscall"
)

type linuxFile struct {
	fd       int
	filename string
}

func (f *linuxFile) Close() error {
	if f.fd != -1 {
		if err := syscall.Close(f.fd); err != nil {
			return err
		}
		f.fd = -1
	}
	return nil
}

func (f *linuxFile) Read(p []byte) (n int, err error) {
	return syscall.Read(f.fd, p)
}

func (f *linuxFile) Write(p []byte) (n int, err error) {
	return syscall.Write(f.fd, p)
}

func (f *linuxFile) Seek(offset int64, whence int) (int64, error) {
	return syscall.Seek(f.fd, offset, whence)
}

func (f *linuxFile) Flush() error {
	return syscall.Fsync(f.fd)
}

func (f *linuxFile) Path() string {
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
	return &linuxFile{fd: fd, filename: filename}, nil
}
