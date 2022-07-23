package storage

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func createHybridStorage(dbFile string, logFile string, l LogFormat) (*HybridStorage, error) {
	wal := Wal{}
	err := wal.Init(logFile, l, false)
	if err != nil {
		return nil, err
	}

	s := HybridStorage{}
	err = s.Init(&wal, "machine0")
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func TestHybridStorageInit(t *testing.T) {
	t.Cleanup(delWalFile)
	delWalFile()

	t.Cleanup(delDBFile)
	delDBFile()

	s, err := createHybridStorage(getDBFile(t), getWalFile(), &JsonLog{})
	assert.Nil(t, err)
	defer s.Close()
}

func TestHybridStorageSave(t *testing.T) {
	t.Cleanup(delWalFile)
	delWalFile()

	t.Cleanup(delDBFile)
	delDBFile()

	s, err := createHybridStorage(getDBFile(t), getWalFile(), &JsonLog{})
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
	assert.Equal(t, testValue+"1", v.Main().value)
	v, err = s.Load(testKey + "2")
	assert.Nil(t, err)
	assert.Equal(t, testValue+"2", v.Main().value)
	v, err = s.Load(testKey + "3")
	assert.Nil(t, err)
	assert.Equal(t, testValue+"3", v.Main().value)
	_, err = s.Load(testKey + "4")
	assert.NotNil(t, err)
}

func TestHybridStorageDel(t *testing.T) {
	t.Cleanup(delWalFile)
	delWalFile()

	t.Cleanup(delDBFile)
	delDBFile()

	s, err := createHybridStorage(getDBFile(t), getWalFile(), &JsonLog{})
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
	assert.Equal(t, testValue+"1", v.Main().value)
	v, err = s.Load(testKey + "2")
	assert.Nil(t, err)
	assert.Equal(t, testValue+"2", v.Main().value)
	v, err = s.Load(testKey + "3")
	assert.Nil(t, err)
	assert.Equal(t, testValue+"3", v.Main().value)
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

	s, err := createHybridStorage(getDBFile(t), getWalFile(), &JsonLog{})
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
	assert.Equal(t, testValue+"1", v.Main().value)
	v, err = s.Load(testKey + "2")
	assert.Nil(t, err)
	assert.Equal(t, testValue+"2", v.Main().value)
	v, err = s.Load(testKey + "3")
	assert.Nil(t, err)
	assert.Equal(t, testValue+"3", v.Main().value)
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

	s, err := createHybridStorage(getDBFile(t), getWalFile(), &JsonLog{})
	assert.Nil(t, err)
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
	assert.Equal(t, testValue+"1", v.Main().value)
	v, err = s.Load(testKey + "2")
	assert.Nil(t, err)
	assert.Equal(t, testValue+"2", v.Main().value)
	v, err = s.Load(testKey + "3")
	assert.Nil(t, err)
	assert.Equal(t, testValue+"3", v.Main().value)
	_, err = s.Load(testKey + "4")
	assert.NotNil(t, err)

	records, err := s.All()
	assert.Nil(t, err)
	expected := [][2]string{{testKey + "1", testValue + "1"},
		{testKey + "2", testValue + "2"},
		{testKey + "3", testValue + "3"}}
	assert.ElementsMatch(t, expected, valuesToArray(records))
}

func TestHybridStorageRecoverDB(t *testing.T) {
	t.Cleanup(delWalFile)
	delWalFile()

	t.Cleanup(delDBFile)
	delDBFile()

	dbFile := getDBFile(t)
	walFile := getWalFile()

	const testKey = "testKey"
	const testValue = "testValue"
	s, err := createHybridStorage(dbFile, walFile, &JsonLog{})
	assert.Nil(t, err)
	err = s.Save(testKey+"1", testValue+"1")
	assert.Nil(t, err)

	err = s.Save(testKey+"2", testValue+"2")
	assert.Nil(t, err)
	err = s.Save(testKey+"3", testValue+"3")
	assert.Nil(t, err)
	err = s.Del(testKey + "2")

	assert.Nil(t, err)
	s.Close()

	s, err = createHybridStorage(dbFile, walFile, &JsonLog{})
	assert.Nil(t, err)
	defer s.Close()
	records, err := s.All()
	assert.Nil(t, err)
	expected := [][2]string{{testKey + "1", testValue + "1"},
		{testKey + "3", testValue + "3"}}
	assert.ElementsMatch(t, expected, valuesToArray(records))
}

func getFileSize(filename string) (int64, error) {
	stat, err := os.Stat(filename)
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

func TestRandomOperationsAndRecovery(t *testing.T) {
	const totalNumOfOperations = 1000

	t.Cleanup(delWalFile)
	delWalFile()
	t.Cleanup(delDBFile)
	delDBFile()
	dbFile := getDBFile(t)
	walFile := getWalFile()

	s, err := createHybridStorage(dbFile, walFile, &BinLog{})
	assert.Nil(t, err)

	key := 0
	expected := map[string]string{}
	for i := 0; i <= totalNumOfOperations; i++ {
		r := rand.Intn(10)
		if r <= 4 { // add a new key--50%
			k := strconv.FormatInt(int64(key), 10)
			v := strconv.FormatInt(rand.Int63(), 10)
			expected[k] = v
			err = s.Save(k, v)
			assert.Nil(t, err)
			key++
		} else if i <= 8 { // modify an existing key--40%
			randomKey := rand.Int63n(int64(key))
			k := strconv.FormatInt(int64(randomKey), 10)
			v := strconv.FormatInt(rand.Int63(), 10)
			expected[k] = v
			err = s.Save(k, v)
			assert.Nil(t, err)
		} else { // deleting a key--10%
			randomKey := rand.Int63n(int64(key))
			k := strconv.FormatInt(int64(randomKey), 10)
			delete(expected, k)
			err = s.Del(k)
			assert.Nil(t, err)
		}
		// fmt.Println(i)
	}

	s.Close()
	delDBFile()

	s, err = createHybridStorage(dbFile, walFile, &BinLog{})
	assert.Nil(t, err)
	defer s.Close()

	records, err := s.All()
	assert.Nil(t, err)
	actual := map[string]string{}
	for _, tuple := range records {
		actual[tuple.Main().key] = tuple.Main().value
	}
	assert.EqualValues(t, expected, actual)

	fmt.Println(len(expected))
	fmt.Println(getFileSize(dbFile))
	fmt.Println(getFileSize(walFile))
}
