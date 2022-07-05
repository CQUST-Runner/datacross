//go:build windows

package storage

import "syscall"

func OpenFile(filename string) (uintptr, error) {
	ptr, err := syscall.UTF16PtrFromString(filename)
	if err != nil {
		return 0, err
	}
	h, err := syscall.CreateFile(ptr, syscall.GENERIC_READ|syscall.GENERIC_WRITE, 0, nil, syscall.OPEN_ALWAYS,
		/*FILE_FLAG_SEQUENTIAL_SCAN*/
		0x08000000, 0)
	syscall.SetFilePointer(h, 0, nil, syscall.FILE_END)
	return uintptr(h), err
}

func CloseFile(fd uintptr) error {
	return syscall.CloseHandle(syscall.Handle(fd))
}

// https://docs.microsoft.com/en-us/windows/win32/fileio/appending-one-file-to-another-file
func WriteFile(fd uintptr, buf []byte) (uint32, error) {
	var done uint32 = 0
	err := syscall.WriteFile(syscall.Handle(fd), buf, &done, nil)
	return done, err
}

func FlushBuffer(fd uintptr) error {
	return syscall.FlushFileBuffers(syscall.Handle(fd))
}
