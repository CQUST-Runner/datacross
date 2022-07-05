package storage

import (
	"io"
)

type File interface {
	io.ReadWriteCloser
	io.Seeker
	Flush() error
}
