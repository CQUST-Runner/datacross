package storage

import "fmt"

// MapWithWal ...
type MapWithWal struct {
}

func (s *MapWithWal) Save(key string, value string) error {
	return nil
}

func (s *MapWithWal) Del(key string) error {
	return nil
}

func (s *MapWithWal) Has(key string) (bool, error) {
	return false, nil
}

func (s *MapWithWal) Load(key string) (string, error) {
	return "", fmt.Errorf("not exist")
}

func (s *MapWithWal) All() ([][2]string, error) {
	return nil, nil
}

func _() {
	var _ Storage = &MapWithWal{}
}
