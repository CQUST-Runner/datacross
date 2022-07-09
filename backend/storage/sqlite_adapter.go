package storage

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gorm.io/driver/sqlite" // Sqlite driver based on GGO

	// "github.com/glebarez/sqlite" // Pure go SQLite driver, checkout https://github.com/glebarez/sqlite for details

	"gorm.io/gorm"
)

type DBRecord struct {
	Key       string // with default column name
	MachineID string `gorm:"column:machine_id"`
	Value     string
	IsMain    bool `gorm:"column:is_main"`
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt sql.NullTime `gorm:"index"`
}

// SqliteAdapter ...
type SqliteAdapter struct {
	db        *gorm.DB
	tableName string
	table     *gorm.DB

	commitID  string
	machineID string
}

const _last_commit_key = "_last_commit"
const _last_sync_key = "_last_sync"

func (s *SqliteAdapter) WithCommitID(id string) Storage {
	return &SqliteAdapter{db: s.db, tableName: s.tableName, table: s.table, commitID: id, machineID: s.machineID}
}

func (s *SqliteAdapter) WithMachineID(id string) Storage {
	return &SqliteAdapter{db: s.db, tableName: s.tableName, table: s.table, commitID: s.commitID, machineID: id}
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

func (s *SqliteAdapter) LastSync() (*SyncStatus, error) {
	jDoc, err := s.Load(_last_sync_key)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return &SyncStatus{}, nil
		}
		return nil, err
	}
	status := SyncStatus{}
	err = json.Unmarshal([]byte(jDoc), &status)
	if err != nil {
		return nil, err
	}
	return &status, nil
}

func (s *SqliteAdapter) Init(dbFile string, tableName string, machineID string) error {
	db, err := gorm.Open(sqlite.Open(dbFile), &gorm.Config{
		SkipDefaultTransaction: true,
	})
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
	s.machineID = machineID
	return nil
}

func (s *SqliteAdapter) Close() error {
	db, err := s.db.DB()
	if err != nil {
		return err
	}
	return db.Close()
}

func withCommitID(table *gorm.DB, machineID string, commitID string, f func(tx *gorm.DB) error) error {
	return table.Transaction(func(tx *gorm.DB) error {
		err := f(tx)
		if err != nil {
			return err
		}
		if len(commitID) > 0 && len(machineID) > 0 {
			return tx.Save(&DBRecord{Key: _last_commit_key, MachineID: machineID, Value: commitID}).Error
		}
		return nil
	})
}

func (s *SqliteAdapter) Save(key string, value string) error {
	if has, err := s.Has(key); has || err != nil {
		return withCommitID(s.table, s.commitID, s.machineID, func(tx *gorm.DB) error {
			return tx.Model(&DBRecord{}).Omit("machine_id").Where("machine_id = ?", s.machineID).Updates(&DBRecord{Key: key, Value: value}).Error
		})
	} else {
		return withCommitID(s.table, s.commitID, s.machineID, func(tx *gorm.DB) error {
			return tx.Create(&DBRecord{Key: key, MachineID: s.machineID, Value: value}).Error
		})
	}
}

func (s *SqliteAdapter) Del(key string) error {
	return withCommitID(s.table, s.commitID, s.machineID, func(tx *gorm.DB) error {
		if len(s.machineID) == 0 {
			return tx.Delete(&DBRecord{Key: key}).Error
		} else {
			return tx.Where("machine_id = ?", s.machineID).Delete(&DBRecord{Key: key}).Error
		}
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
	result := s.table.Where("machine_id = ?", s.machineID).First(&rec, "key = ?", key)
	if result.Error != nil {
		return "", result.Error
	}
	return rec.Value, nil
}

func (s *SqliteAdapter) All() ([][2]string, error) {
	records := []DBRecord{}
	var result *gorm.DB
	if len(s.machineID) > 0 {
		result = s.table.Where("machine_id = ?", s.machineID).Find(&records)
	} else {
		result = s.table.Find(&records)
	}
	if result.Error != nil {
		return nil, result.Error
	}
	kvs := [][2]string{}
	for _, rec := range records {
		if rec.Key == _last_commit_key || rec.Key == _last_sync_key {
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
