package storage

import (
	"fmt"
	"testing"
)

// func TestOpenFile(t *testing.T) {
// 	fd, err := OpenFile("test.txt")
// 	assert.Nil(t, err)
// 	fmt.Println(fd)
// 	CloseFile(fd)
// }

// func TestWriteFile(t *testing.T) {
// 	fd, err := OpenFile("test.txt")
// 	assert.Nil(t, err)
// 	fmt.Println(fd)
// 	defer CloseFile(fd)

// 	n, err := WriteFile(fd, []byte("hello world"))
// 	assert.Nil(t, err)
// 	fmt.Println(n)
// 	n, err = WriteFile(fd, []byte("hello world"))
// 	assert.Nil(t, err)
// 	fmt.Println(n)
// }

func TestGenUUID(t *testing.T) {
	fmt.Println(GenUUID())
}
