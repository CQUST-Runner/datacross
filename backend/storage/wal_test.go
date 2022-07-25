package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

const walFileName = "test.wal"

func getWalFile() string {
	delWalFile()
	return walFileName
}

func delWalFile() {
	os.RemoveAll("./data")

	_, err := os.Stat(walFileName)
	if err != nil {
		return
	}
	err = os.Remove(walFileName)
	if err != nil {
		fmt.Println(err)
	}
}

func testWalInit(t assert.TestingT, l LogFormat) {
	wal := Wal{}
	err := wal.Init(walFileName, l, false)
	assert.Nil(t, err)

	defer wal.Close()
}

func TestWalInitWithBinLog(t *testing.T) {
	delWalFile()
	t.Cleanup(delWalFile)

	testWalInit(t, &BinLog{})
}

func TestWalInitWithJsonLog(t *testing.T) {
	delWalFile()
	t.Cleanup(delWalFile)

	testWalInit(t, &JsonLog{})
}

func testWalAppend(t assert.TestingT, l LogFormat) {
	wal := Wal{}
	err := wal.Init(walFileName, l, false)
	assert.Nil(t, err)
	defer wal.Close()

	const testKey = "testKey"
	const testValue = "testValue"
	_, _, err = wal.Append(&LogOperation{Op: int32(Op_Modify), Key: testKey + "1", Value: testValue + "1"})
	assert.Nil(t, err)
	_, _, err = wal.Append(&LogOperation{Op: int32(Op_Modify), Key: testKey + "2", Value: testValue + "2"})
	assert.Nil(t, err)
	_, _, err = wal.Append(&LogOperation{Op: int32(Op_Modify), Key: testKey + "3", Value: testValue + "3"})
	assert.Nil(t, err)
	_, _, err = wal.Append(&LogOperation{Op: int32(Op_Modify), Key: testKey + "3", Value: testValue + "4"})
	assert.Nil(t, err)
	_, _, err = wal.Append(&LogOperation{Op: int32(Op_Del), Key: testKey + "2", Value: ""})
	assert.Nil(t, err)

	// m := newMapWrapper()
	// err = wal.Replay(m, "")
	// assert.Nil(t, err)
	// expected := map[string]string{testKey + "1": testValue + "1", testKey + "3": testValue + "4"}
	// assert.Equal(t, fmt.Sprint(expected), fmt.Sprint(m.m))

	// _, _, err = wal.Append(&LogOperation{Op: int32(Op_Modify), Key: testKey + "4", Value: testValue + "4"})
	// assert.Nil(t, err)
	// _, _, err = wal.Append(&LogOperation{Op: int32(Op_Del), Key: testKey + "1", Value: ""})
	// assert.Nil(t, err)

	// m = newMapWrapper()
	// err = wal.Replay(m, "")
	// assert.Nil(t, err)
	// expected = map[string]string{testKey + "3": testValue + "4", testKey + "4": testValue + "4"}
	// assert.Equal(t, fmt.Sprint(expected), fmt.Sprint(m.m))
}

func TestWalAppendWithBinLog(t *testing.T) {
	delWalFile()
	t.Cleanup(delWalFile)

	testWalAppend(t, &BinLog{})
}

func TestWalAppendWithJsonLog(t *testing.T) {
	delWalFile()
	t.Cleanup(delWalFile)

	testWalAppend(t, &JsonLog{})
}

func TestWalIterator(t *testing.T) {
	delWalFile()
	t.Cleanup(delWalFile)

	wal := Wal{}
	err := wal.Init(walFileName, &JsonLog{}, false)
	assert.Nil(t, err)
	defer wal.Close()

	const testKey = "testKey"
	const testValue = "testValue"
	_, _, err = wal.Append(&LogOperation{Op: int32(Op_Modify), Key: testKey + "1", Value: testValue + "1"})
	assert.Nil(t, err)
	_, _, err = wal.Append(&LogOperation{Op: int32(Op_Modify), Key: testKey + "2", Value: testValue + "2"})
	assert.Nil(t, err)
	_, _, err = wal.Append(&LogOperation{Op: int32(Op_Modify), Key: testKey + "3", Value: testValue + "3"})
	assert.Nil(t, err)
	_, _, err = wal.Append(&LogOperation{Op: int32(Op_Modify), Key: testKey + "3", Value: testValue + "4"})
	assert.Nil(t, err)
	_, _, err = wal.Append(&LogOperation{Op: int32(Op_Del), Key: testKey + "1", Value: ""},
		&LogOperation{Op: int32(Op_Del), Key: testKey + "2", Value: ""})
	assert.Nil(t, err)

	num := 0
	i := wal.Iterator()
	for i.Next() {
		fmt.Println(i.LogOp())
		num++
	}
	assert.Equal(t, 6, num)
}

func TestWalIteratorFrom(t *testing.T) {
	delWalFile()
	t.Cleanup(delWalFile)

	wal := Wal{}
	err := wal.Init(walFileName, &JsonLog{}, false)
	assert.Nil(t, err)
	defer wal.Close()

	const testKey = "testKey"
	const testValue = "testValue"
	_, _, err = wal.Append(&LogOperation{Op: int32(Op_Modify), Key: testKey + "1", Value: testValue + "1"})
	assert.Nil(t, err)
	_, _, err = wal.Append(&LogOperation{Op: int32(Op_Modify), Key: testKey + "2", Value: testValue + "2"})
	assert.Nil(t, err)
	gid, _, err := wal.Append(&LogOperation{Op: int32(Op_Modify), Key: testKey + "3", Value: testValue + "3"})
	assert.Nil(t, err)
	_, _, err = wal.Append(&LogOperation{Op: int32(Op_Modify), Key: testKey + "3", Value: testValue + "4"})
	assert.Nil(t, err)
	_, _, err = wal.Append(&LogOperation{Op: int32(Op_Del), Key: testKey + "1", Value: ""},
		&LogOperation{Op: int32(Op_Del), Key: testKey + "2", Value: ""})
	assert.Nil(t, err)

	num := 0
	i, err := wal.IteratorFrom(gid, false)
	assert.Nil(t, err)
	for i.Next() {
		fmt.Println(i.LogOp())
		num++
	}
	assert.Equal(t, 3, num)
}

func TestWalRangeIterator(t *testing.T) {
	delWalFile()
	t.Cleanup(delWalFile)

	wal := Wal{}
	err := wal.Init(walFileName, &JsonLog{}, false)
	assert.Nil(t, err)
	defer wal.Close()

	const testKey = "testKey"
	const testValue = "testValue"
	_, _, err = wal.Append(&LogOperation{Op: int32(Op_Modify), Key: testKey + "1", Value: testValue + "1"})
	assert.Nil(t, err)
	_, _, err = wal.Append(&LogOperation{Op: int32(Op_Modify), Key: testKey + "2", Value: testValue + "2"})
	assert.Nil(t, err)
	gid, _, err := wal.Append(&LogOperation{Op: int32(Op_Modify), Key: testKey + "3", Value: testValue + "3"})
	assert.Nil(t, err)
	_, _, err = wal.Append(&LogOperation{Op: int32(Op_Modify), Key: testKey + "3", Value: testValue + "4"})
	assert.Nil(t, err)
	gid2, _, err := wal.Append(&LogOperation{Op: int32(Op_Del), Key: testKey + "1", Value: ""},
		&LogOperation{Op: int32(Op_Del), Key: testKey + "2", Value: ""})
	assert.Nil(t, err)

	num := 0
	i, err := wal.RangeIterator(gid, gid2, true, false)
	assert.Nil(t, err)
	for i.Next() {
		fmt.Println(i.LogOp())
		num++
	}
	assert.Equal(t, 3, num)
}
