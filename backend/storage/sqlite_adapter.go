package storage

import "fmt"

// SqliteAdapter ...
type SqliteAdapter struct {
}

func (s *SqliteAdapter) Save(key string, value string) error {
	return nil
}

func (s *SqliteAdapter) Del(key string) error {
	return nil
}

func (s *SqliteAdapter) Has(key string) (bool, error) {
	return false, nil
}

func (s *SqliteAdapter) Load(key string) (string, error) {
	return "", fmt.Errorf("not exist")
}
