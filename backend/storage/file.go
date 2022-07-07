package storage

import (
	"io"
)

// 不要依赖文件内部维护的position
type File interface {
	io.ReadWriteCloser
	io.Seeker
	Flush() error
}
