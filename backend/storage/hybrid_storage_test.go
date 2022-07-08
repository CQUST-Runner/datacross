package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHybridStorageInit(t *testing.T) {
	t.Cleanup(delWalFile)
	delWalFile()

	t.Cleanup(delDBFile)
	delDBFile()

	s := HybridStorage{}
	err := s.Init(getDBFile(t), getWalFile(), &JsonLog{})
	assert.Nil(t, err)
	defer s.Close()
}

func TestHybridStorageSave(t *testing.T) {
	t.Cleanup(delWalFile)
	delWalFile()

	t.Cleanup(delDBFile)
	delDBFile()

	s := HybridStorage{}
	err := s.Init(getDBFile(t), getWalFile(), &JsonLog{})
	assert.Nil(t, err)
	defer s.Close()

	const testKey = "testKey"
	const testValue = "testValue"
	err = s.Save(testKey+"1", testValue+"1")
	assert.Nil(t, err)
	err = s.Save(testKey+"2", testValue+"2")
	assert.Nil(t, err)
	err = s.Save(testKey+"3", testValue+"3")
	assert.Nil(t, err)

	v, err := s.Load(testKey + "1")
	assert.Nil(t, err)
	assert.Equal(t, testValue+"1", v)
	v, err = s.Load(testKey + "2")
	assert.Nil(t, err)
	assert.Equal(t, testValue+"2", v)
	v, err = s.Load(testKey + "3")
	assert.Nil(t, err)
	assert.Equal(t, testValue+"3", v)
	_, err = s.Load(testKey + "4")
	assert.NotNil(t, err)
}

func TestHybridStorageDel(t *testing.T) {
	t.Cleanup(delWalFile)
	delWalFile()

	t.Cleanup(delDBFile)
	delDBFile()

	s := HybridStorage{}
	err := s.Init(getDBFile(t), getWalFile(), &JsonLog{})
	assert.Nil(t, err)
	defer s.Close()

	const testKey = "testKey"
	const testValue = "testValue"
	err = s.Save(testKey+"1", testValue+"1")
	assert.Nil(t, err)
	err = s.Save(testKey+"2", testValue+"2")
	assert.Nil(t, err)
	err = s.Save(testKey+"3", testValue+"3")
	assert.Nil(t, err)

	v, err := s.Load(testKey + "1")
	assert.Nil(t, err)
	assert.Equal(t, testValue+"1", v)
	v, err = s.Load(testKey + "2")
	assert.Nil(t, err)
	assert.Equal(t, testValue+"2", v)
	v, err = s.Load(testKey + "3")
	assert.Nil(t, err)
	assert.Equal(t, testValue+"3", v)
	_, err = s.Load(testKey + "4")
	assert.NotNil(t, err)

	err = s.Del(testKey + "3")
	assert.Nil(t, err)
	_, err = s.Load(testKey + "3")
	assert.NotNil(t, err)
}

func TestHybridStorageHas(t *testing.T) {
	t.Cleanup(delWalFile)
	delWalFile()

	t.Cleanup(delDBFile)
	delDBFile()

	s := HybridStorage{}
	err := s.Init(getDBFile(t), getWalFile(), &JsonLog{})
	assert.Nil(t, err)
	defer s.Close()

	const testKey = "testKey"
	const testValue = "testValue"
	err = s.Save(testKey+"1", testValue+"1")
	assert.Nil(t, err)
	err = s.Save(testKey+"2", testValue+"2")
	assert.Nil(t, err)
	err = s.Save(testKey+"3", testValue+"3")
	assert.Nil(t, err)

	v, err := s.Load(testKey + "1")
	assert.Nil(t, err)
	assert.Equal(t, testValue+"1", v)
	v, err = s.Load(testKey + "2")
	assert.Nil(t, err)
	assert.Equal(t, testValue+"2", v)
	v, err = s.Load(testKey + "3")
	assert.Nil(t, err)
	assert.Equal(t, testValue+"3", v)
	_, err = s.Load(testKey + "4")
	assert.NotNil(t, err)

	has, err := s.Has(testKey + "1")
	assert.Nil(t, err)
	assert.True(t, has)
	has, err = s.Has(testKey + "4")
	assert.Nil(t, err)
	assert.False(t, has)
}

func TestHybridStorageAll(t *testing.T) {
	t.Cleanup(delWalFile)
	delWalFile()

	t.Cleanup(delDBFile)
	delDBFile()

	s := HybridStorage{}
	err := s.Init(getDBFile(t), getWalFile(), &JsonLog{})
	assert.Nil(t, err)
	defer s.Close()

	const testKey = "testKey"
	const testValue = "testValue"
	err = s.Save(testKey+"1", testValue+"1")
	assert.Nil(t, err)
	err = s.Save(testKey+"2", testValue+"2")
	assert.Nil(t, err)
	err = s.Save(testKey+"3", testValue+"3")
	assert.Nil(t, err)

	v, err := s.Load(testKey + "1")
	assert.Nil(t, err)
	assert.Equal(t, testValue+"1", v)
	v, err = s.Load(testKey + "2")
	assert.Nil(t, err)
	assert.Equal(t, testValue+"2", v)
	v, err = s.Load(testKey + "3")
	assert.Nil(t, err)
	assert.Equal(t, testValue+"3", v)
	_, err = s.Load(testKey + "4")
	assert.NotNil(t, err)

	records, err := s.All()
	assert.Nil(t, err)
	expected := [][2]string{{testKey + "1", testValue + "1"},
		{testKey + "2", testValue + "2"},
		{testKey + "3", testValue + "3"}}
	assert.EqualValues(t, expected, records)
}
