package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

const walFileName = "test.wal"

func delWalFile() {
	_, err := os.Stat(walFileName)
	if err != nil {
		return
	}
	err = os.Remove(walFileName)
	if err != nil {
		fmt.Println(err)
	}
}

func testWalInit(t assert.TestingT, l LogFile) {
	wal := Wal{}
	err := wal.Init(walFileName, l)
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

type mapWrapper struct {
	m map[string]string
}

func newMapWrapper() *mapWrapper {
	return &mapWrapper{m: make(map[string]string)}
}

func (s *mapWrapper) WithCommitID(_ string) Storage {
	return s
}

func (s *mapWrapper) Save(key string, value string) error {
	s.m[key] = value
	return nil
}

func (s *mapWrapper) Del(key string) error {
	delete(s.m, key)
	return nil
}

func (s *mapWrapper) Has(key string) (bool, error) {
	_, ok := s.m[key]
	return ok, nil
}

func (s *mapWrapper) Load(key string) (string, error) {
	val, ok := s.m[key]
	if ok {
		return val, nil
	}
	return "", fmt.Errorf("not exist")
}

func (s *mapWrapper) All() ([][2]string, error) {
	records := [][2]string{}
	for k, v := range s.m {
		records = append(records, [2]string{k, v})
	}
	return records, nil
}

func _() {
	var _ Storage = &mapWrapper{}
}

func testWalAppend(t assert.TestingT, l LogFile) {
	wal := Wal{}
	err := wal.Init(walFileName, l)
	assert.Nil(t, err)
	defer wal.Close()

	const testKey = "testKey"
	const testValue = "testValue"
	_, err = wal.Append(int32(Op_Add), testKey+"1", testValue+"1")
	assert.Nil(t, err)
	_, err = wal.Append(int32(Op_Add), testKey+"2", testValue+"2")
	assert.Nil(t, err)
	_, err = wal.Append(int32(Op_Add), testKey+"3", testValue+"3")
	assert.Nil(t, err)
	_, err = wal.Append(int32(Op_Modify), testKey+"3", testValue+"4")
	assert.Nil(t, err)
	_, err = wal.Append(int32(Op_Del), testKey+"2", "")
	assert.Nil(t, err)

	m := newMapWrapper()
	err = wal.Replay(m, "")
	assert.Nil(t, err)
	expected := map[string]string{testKey + "1": testValue + "1", testKey + "3": testValue + "4"}
	assert.Equal(t, fmt.Sprint(expected), fmt.Sprint(m.m))

	_, err = wal.Append(int32(Op_Add), testKey+"4", testValue+"4")
	assert.Nil(t, err)
	_, err = wal.Append(int32(Op_Del), testKey+"1", "")
	assert.Nil(t, err)

	m = newMapWrapper()
	err = wal.Replay(m, "")
	assert.Nil(t, err)
	expected = map[string]string{testKey + "3": testValue + "4", testKey + "4": testValue + "4"}
	assert.Equal(t, fmt.Sprint(expected), fmt.Sprint(m.m))
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
