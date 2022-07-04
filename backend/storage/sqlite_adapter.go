package storage

import (
	"gorm.io/driver/sqlite" // Sqlite driver based on GGO
	// "github.com/glebarez/sqlite" // Pure go SQLite driver, checkout https://github.com/glebarez/sqlite for details
	"fmt"

	"gorm.io/gorm"
)

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

func foo() {

	// github.com/mattn/go-sqlite3
	db, err := gorm.Open(sqlite.Open("gorm.db"), &gorm.Config{})
	_ = db
	_ = err
}
