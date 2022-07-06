package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func getDB(t assert.TestingT) *SqliteAdapter {
	const fileName = "test.db"
	const tableName = "test"
	err := os.Remove(fileName)
	assert.Nil(t, err)

	a := SqliteAdapter{}
	err = a.Init(fileName, tableName)
	assert.Nil(t, err)
	return &a
}

func TestInit(t *testing.T) {
	a := getDB(t)
	_ = a
}

func TestSave(t *testing.T) {
	a := getDB(t)

	const key = "test1"
	const valuePrefix = "testVal"

	err := a.Save(key, valuePrefix+"1")
	assert.Nil(t, err)

	err = a.Save(key, valuePrefix+"2")
	assert.Nil(t, err)
}

func TestLoad(t *testing.T) {
	a := getDB(t)

	const key = "test1"
	const valuePrefix = "testVal"

	err := a.Save(key, valuePrefix+"1")
	assert.Nil(t, err)

	val, err := a.Load(key)
	assert.Nil(t, err)
	assert.Equal(t, valuePrefix+"1", val)

	err = a.Save(key, valuePrefix+"2")
	assert.Nil(t, err)

	val, err = a.Load(key)
	assert.Nil(t, err)
	assert.Equal(t, valuePrefix+"2", val)
}

func TestHas(t *testing.T) {
	a := getDB(t)

	const key = "test1"
	const valuePrefix = "testVal"

	has, err := a.Has(key)
	assert.Nil(t, err)
	assert.False(t, has)

	err = a.Save(key, valuePrefix+"1")
	assert.Nil(t, err)

	has, err = a.Has(key)
	assert.Nil(t, err)
	assert.True(t, has)
}

func TestDel(t *testing.T) {
	a := getDB(t)

	const key = "test1"
	const valuePrefix = "testVal"

	err := a.Save(key, valuePrefix+"1")
	assert.Nil(t, err)

	has, err := a.Has(key)
	assert.Nil(t, err)
	assert.True(t, has)

	err = a.Del(key)
	assert.Nil(t, err)

	has, err = a.Has(key)
	assert.Nil(t, err)
	assert.False(t, has)
}

func TestAll(t *testing.T) {
	a := getDB(t)

	const key1 = "test1"
	const key2 = "test2"
	const valuePrefix = "testVal"

	err := a.Save(key1, valuePrefix+"1")
	assert.Nil(t, err)

	err = a.Save(key2, valuePrefix+"2")
	assert.Nil(t, err)

	records, err := a.All()
	assert.Nil(t, err)
	m := map[string]string{}
	for _, rec := range records {
		if len(rec) >= 2 {
			m[rec[0]] = rec[1]
		}
	}

	_, ok := m[key1]
	assert.True(t, ok)
	assert.Equal(t, valuePrefix+"1", m[key1])
	_, ok = m[key2]
	assert.True(t, ok)
	assert.Equal(t, valuePrefix+"2", m[key2])
}
