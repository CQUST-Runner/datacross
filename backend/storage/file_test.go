package storage

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFile(t *testing.T) {
	defer os.Remove("test.txt")
	f, err := OpenFile("test.txt")
	assert.Nil(t, err)
	assert.NotNil(t, f)
	defer f.Close()

	pos, err := f.Seek(0, io.SeekStart)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), pos)

	n, err := f.Write([]byte("hello"))
	assert.Nil(t, err)
	assert.Equal(t, 5, n)

	pos, err = f.Seek(0, io.SeekStart)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), pos)

	buf := [100]byte{}
	n, err = f.Read(buf[:])
	assert.Nil(t, err)
	fmt.Println(n)
	fmt.Println(string(buf[:n]))
}
