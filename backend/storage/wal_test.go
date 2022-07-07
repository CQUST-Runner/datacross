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

func TestWalInit(t *testing.T) {
	delWalFile()
	t.Cleanup(delWalFile)

	wal := Wal{}
	err := wal.Init(walFileName, &BinLog{})
	assert.Nil(t, err)

	defer wal.Close()
}

type mapWrapper struct {
	m map[string]string
}

func newMapWrapper() *mapWrapper {
	return &mapWrapper{m: make(map[string]string)}
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

func TestWalAppend(t *testing.T) {
	delWalFile()
	t.Cleanup(delWalFile)

	wal := Wal{}
	err := wal.Init(walFileName, &BinLog{})
	assert.Nil(t, err)
	defer wal.Close()

	const testKey = "testKey"
	const testValue = "testValue"
	err = wal.Append(&LogEntry{Op: int32(Op_Add), Key: testKey + "1", Value: testValue + "1"})
	assert.Nil(t, err)
	err = wal.Append(&LogEntry{Op: int32(Op_Add), Key: testKey + "2", Value: testValue + "2"})
	assert.Nil(t, err)
	err = wal.Append(&LogEntry{Op: int32(Op_Add), Key: testKey + "3", Value: testValue + "3"})
	assert.Nil(t, err)
	err = wal.Append(&LogEntry{Op: int32(Op_Modify), Key: testKey + "3", Value: testValue + "4"})
	assert.Nil(t, err)
	err = wal.Append(&LogEntry{Op: int32(Op_Del), Key: testKey + "2"})
	assert.Nil(t, err)

	m := newMapWrapper()
	err = wal.Replay(m)
	assert.Nil(t, err)
	expected := map[string]string{testKey + "1": testValue + "1", testKey + "3": testValue + "4"}
	assert.Equal(t, fmt.Sprint(expected), fmt.Sprint(m.m))

	err = wal.Append(&LogEntry{Op: int32(Op_Add), Key: testKey + "4", Value: testValue + "4"})
	assert.Nil(t, err)
	err = wal.Append(&LogEntry{Op: int32(Op_Del), Key: testKey + "1"})
	assert.Nil(t, err)

	m = newMapWrapper()
	err = wal.Replay(m)
	assert.Nil(t, err)
	expected = map[string]string{testKey + "3": testValue + "4", testKey + "4": testValue + "4"}
	assert.Equal(t, fmt.Sprint(expected), fmt.Sprint(m.m))
}
