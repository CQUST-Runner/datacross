package storage

import "fmt"

// Storage ...
type Storage interface {
	Save(key string, value string) error
	Del(key string) error
	Has(key string) (bool, error)
	Load(key string) (val string, err error)
	All() ([][2]string, error)
}

// HybridStorage ...
type HybridStorage struct {
}

func (s *HybridStorage) Save(key string, value string) error {
	return nil
}

func (s *HybridStorage) Del(key string) error {
	return nil
}

func (s *HybridStorage) Has(key string) (bool, error) {
	return false, nil
}

func (s *HybridStorage) Load(key string) (string, error) {
	return "", fmt.Errorf("not exist")
}

func (s *HybridStorage) All() ([][2]string, error) {
	return nil, nil
}

func _() {
	var _ Storage = &HybridStorage{}
}
