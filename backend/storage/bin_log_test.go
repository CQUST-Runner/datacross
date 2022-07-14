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

	f, err := OpenFile(_fileName, false)
	assert.Nil(t, err)
	return f
}

func delFile() {
	err := os.Remove(_fileName)
	if err != nil {
		fmt.Println(err)
	}
}

func testHeader(t assert.TestingT, l LogFormat) {
	f := getFile(t)
	defer f.Close()
	fileID := "test"

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

func TestHeader(t *testing.T) {
	t.Cleanup(delFile)
	testHeader(t, &BinLog{})
}

func testLogEntry(t assert.TestingT, l LogFormat) {
	f := getFile(t)
	defer f.Close()
	fileID := "test"

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
	logOp := LogOperation{Op: int32(Op_Modify), Key: testKey + "1", Value: testValue + "1"}
	n, err := l.AppendEntry(f, -1, &LogEntry{Ops: []*LogOperation{&logOp}})
	assert.Nil(t, err)
	assert.Greater(t, n, int64(8))

	logOp = LogOperation{Op: int32(Op_Modify), Key: testKey + "2", Value: testValue + "2"}
	n, err = l.AppendEntry(f, -1, &LogEntry{Ops: []*LogOperation{&logOp}})
	assert.Nil(t, err)
	assert.Greater(t, n, int64(8))

	entry := LogEntry{}
	n, err = l.ReadEntry(f, HeaderSize, &entry)
	assert.Nil(t, err)
	assert.Greater(t, n, int64(8))
	assert.Equal(t, 1, len(entry.Ops))
	logOp = *entry.Ops[0]
	assert.Equal(t, int32(Op_Modify), logOp.Op)
	assert.Equal(t, testKey+"1", logOp.Key)
	assert.Equal(t, testValue+"1", logOp.Value)

	entry = LogEntry{}
	n, err = l.ReadEntry(f, HeaderSize+n, &entry)
	assert.Nil(t, err)
	assert.Greater(t, n, int64(8))
	assert.Equal(t, 1, len(entry.Ops))
	logOp = *entry.Ops[0]
	assert.Equal(t, int32(Op_Modify), logOp.Op)
	assert.Equal(t, testKey+"2", logOp.Key)
	assert.Equal(t, testValue+"2", logOp.Value)
}

func TestLogEntry(t *testing.T) {
	t.Cleanup(delFile)
	testLogEntry(t, &BinLog{})
}
