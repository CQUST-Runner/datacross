package storage

import (
	"fmt"
)

// MapWithWal ...
type MapWithWal struct {
	log *Wal
	m   map[string]string
}

func (s *MapWithWal) Init(log *Wal) {
	s.log = log
	s.m = map[string]string{}
}

func (s *MapWithWal) Save(key string, value string) error {
	err := s.log.Append(int32(Op_Modify), key, value)
	if err != nil {
		return err
	}
	s.m[key] = value
	return nil
}

func (s *MapWithWal) Del(key string) error {
	err := s.log.Append(int32(Op_Del), key, "")
	if err != nil {
		return err
	}
	delete(s.m, key)
	return nil
}

func (s *MapWithWal) Has(key string) (bool, error) {
	if _, ok := s.m[key]; ok {
		return true, nil
	}
	return false, nil
}

func (s *MapWithWal) Load(key string) (string, error) {
	if val, ok := s.m[key]; ok {
		return val, nil
	}
	return "", fmt.Errorf("not exist")
}

func (s *MapWithWal) All() ([][2]string, error) {
	records := [][2]string{}
	for k, v := range s.m {
		records = append(records, [2]string{k, v})
	}
	return records, nil
}

func _() {
	var _ Storage = &MapWithWal{}
}
