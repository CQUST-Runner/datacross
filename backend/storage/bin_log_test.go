package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

const _fileName = "test"

func getFile(t assert.TestingT) File {
	_, err := os.Stat(_fileName)
	if err == nil {
		err = os.Remove(_fileName)
		assert.Nil(t, err)
	}

	f, err := OpenFile(_fileName)
	assert.Nil(t, err)
	return f
}

func delFile() {
	err := os.Remove(_fileName)
	if err != nil {
		fmt.Println(err)
	}
}

func TestHeader(t *testing.T) {
	t.Cleanup(delFile)
	f := getFile(t)
	defer f.Close()
	fileID := "test"

	l := BinLog{}
	valid, err := l.IsValidFile(f)
	assert.Nil(t, err)
	assert.False(t, valid)

	header := FileHeader{Id: fileID, FileEnd: HeaderSize}
	err = l.WriteHeader(f, &header)
	assert.Nil(t, err)

	valid, err = l.IsValidFile(f)
	assert.Nil(t, err)
	assert.True(t, valid)

	h, err := l.ReadHeader(f)
	assert.Nil(t, err)
	assert.Equal(t, fileID, h.Id)
	assert.Equal(t, int64(HeaderSize), h.FileEnd)
}

func TestLogEntry(t *testing.T) {
	t.Cleanup(delFile)
	f := getFile(t)
	defer f.Close()
	fileID := "test"

	l := BinLog{}
	valid, err := l.IsValidFile(f)
	assert.Nil(t, err)
	assert.False(t, valid)

	header := FileHeader{Id: fileID, FileEnd: HeaderSize}
	err = l.WriteHeader(f, &header)
	assert.Nil(t, err)

	valid, err = l.IsValidFile(f)
	assert.Nil(t, err)
	assert.True(t, valid)

	testKey := "testKey"
	testValue := "testValue"
	entry := LogEntry{Op: int32(Op_Modify), Key: testKey + "1", Value: testValue + "1"}
	err = l.AppendEntry(f, -1, &entry)
	assert.Nil(t, err)

	entry = LogEntry{Op: int32(Op_Modify), Key: testKey + "2", Value: testValue + "2"}
	err = l.AppendEntry(f, -1, &entry)
	assert.Nil(t, err)

	entry = LogEntry{}
	n, err := l.ReadEntry(f, HeaderSize, &entry)
	assert.Nil(t, err)
	assert.Greater(t, n, int64(8))
	assert.Equal(t, int32(Op_Modify), entry.Op)
	assert.Equal(t, testKey+"1", entry.Key)
	assert.Equal(t, testValue+"1", entry.Value)

	entry = LogEntry{}
	n, err = l.ReadEntry(f, HeaderSize+n, &entry)
	assert.Nil(t, err)
	assert.Greater(t, n, int64(8))
	assert.Equal(t, int32(Op_Modify), entry.Op)
	assert.Equal(t, testKey+"2", entry.Key)
	assert.Equal(t, testValue+"2", entry.Value)
}
