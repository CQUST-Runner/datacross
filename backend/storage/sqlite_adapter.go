package storage

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"gorm.io/driver/sqlite" // Sqlite driver based on GGO

	// "github.com/glebarez/sqlite" // Pure go SQLite driver, checkout https://github.com/glebarez/sqlite for details

	"gorm.io/gorm"
)

type DBRecord struct {
	Key       string `gorm:"primarykey"`
	MachineID string
	Value     string
	IsMain    bool
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt sql.NullTime `gorm:"index"`
}

// SqliteAdapter ...
type SqliteAdapter struct {
	db        *gorm.DB
	tableName string
	table     *gorm.DB

	commitID string
}

const _last_commit_key = "_last_commit"

func (s *SqliteAdapter) WithCommitID(id string) Storage {
	return &SqliteAdapter{db: s.db, tableName: s.tableName, table: s.table, commitID: id}
}

func (s *SqliteAdapter) LastCommit() (string, error) {
	id, err := s.Load(_last_commit_key)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", nil
		}
		return "", err
	}
	return id, nil
}

func (s *SqliteAdapter) Init(dbFile string, tableName string) error {
	db, err := gorm.Open(sqlite.Open(dbFile), &gorm.Config{})
	if err != nil {
		return err
	}

	err = db.AutoMigrate(&DBRecord{})
	if err != nil {
		return err
	}

	// s.table = db.Table(tableName)
	s.table = db
	s.tableName = tableName
	s.db = db
	return nil
}

func (s *SqliteAdapter) Close() error {
	db, err := s.db.DB()
	if err != nil {
		return err
	}
	db.Close()
	return nil
}

func withCommitID(table *gorm.DB, commitID string, f func(tx *gorm.DB) error) error {
	return table.Transaction(func(tx *gorm.DB) error {
		err := f(tx)
		if err != nil {
			return err
		}
		return tx.Save(&DBRecord{Key: _last_commit_key, Value: commitID}).Error
	})
}

func (s *SqliteAdapter) Save(key string, value string) error {
	if has, err := s.Has(key); has || err != nil {
		return withCommitID(s.table, s.commitID, func(tx *gorm.DB) error {
			return s.table.Save(&DBRecord{Key: key, MachineID: "", Value: value}).Error
		})
	} else {
		return withCommitID(s.table, s.commitID, func(tx *gorm.DB) error {
			return s.table.Create(&DBRecord{Key: key, MachineID: "", Value: value}).Error
		})
	}
}

func (s *SqliteAdapter) Del(key string) error {
	return withCommitID(s.table, s.commitID, func(tx *gorm.DB) error {
		return s.table.Delete(&DBRecord{Key: key}).Error
	})
}

func (s *SqliteAdapter) Has(key string) (bool, error) {
	recs := []DBRecord{}
	result := s.table.Find(&recs, "key = ?", key)
	if result.Error != nil {
		return false, result.Error
	}

	return result.RowsAffected > 0, nil
}

func (s *SqliteAdapter) Load(key string) (string, error) {
	rec := DBRecord{}
	result := s.table.First(&rec, "key = ?", key)
	if result.Error != nil {
		return "", result.Error
	}
	return rec.Value, nil
}

func (s *SqliteAdapter) All() ([][2]string, error) {
	records := []DBRecord{}
	result := s.table.Find(&records)
	if result.Error != nil {
		return nil, result.Error
	}
	kvs := [][2]string{}
	for _, rec := range records {
		if rec.Key == _last_commit_key {
			continue
		}
		kvs = append(kvs, [2]string{rec.Key, rec.Value})
	}
	return kvs, nil
}

func (s *SqliteAdapter) Merge(Storage) error {
	return fmt.Errorf("unsupported")
}

func _() {
	var _ Storage = &SqliteAdapter{}
}
