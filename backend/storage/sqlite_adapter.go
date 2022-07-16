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

// is_deleted || is_discarded can be removed from storage any time
type DBRecord struct {
	Key           string `gorm:"column:key"`
	Value         string `gorm:"column:value"`
	MachineID     string `gorm:"column:machine_id"`
	PrevMachineID string `gorm:"column:prev_machine_id"`
	Seq           uint64 `gorm:"column:seq"`
	CurrentLogGid string `gorm:"column:current_log_gid"`
	PrevLogGid    string `gorm:"column:prev_log_gid"`
	IsDiscarded   bool   `gorm:"column:is_discarded"`
	IsDeleted     bool   `gorm:"column:is_deleted"`
	// TODO how to determine
	IsMain    bool `gorm:"column:is_main"`
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt sql.NullTime `gorm:"index"`
}

func (r *DBRecord) Visible() bool {
	return !r.IsDeleted && !r.IsDiscarded
}

type SyncStatus struct {
	Pos       map[string]string
	Time      time.Time
	MachineID string
}

func (s *SyncStatus) Position(name string) string {
	position, ok := s.Pos[name]
	if !ok {
		return ""
	}
	return position
}

func (s *SyncStatus) Merge(other *SyncStatus) {
	for id, pos := range other.Pos {
		s.Pos[id] = pos
	}
	s.Time = other.Time
}

// SqliteAdapter ...
type SqliteAdapter struct {
	db        *gorm.DB
	tableName string

	workingDB *gorm.DB
	commitID  string
	machineID string
}

const _last_commit_key = "_last_commit"
const _last_sync_key = "_last_sync"

func (s *SqliteAdapter) WithCommitID(id string) Storage {
	return &SqliteAdapter{db: s.db, tableName: s.tableName, workingDB: s.workingDB, commitID: id, machineID: s.machineID}
}

func (s *SqliteAdapter) WithMachineID(id string) Storage {
	return &SqliteAdapter{db: s.db, tableName: s.tableName, workingDB: s.workingDB, commitID: s.commitID, machineID: id}
}

func (s *SqliteAdapter) LastCommit() (string, error) {
	if len(s.machineID) == 0 {
		return "", fmt.Errorf("must specify a machine id")
	}

	rec := DBRecord{}
	result := s.workingDB.Where("machine_id = ? AND key = ?", s.machineID, _last_commit_key).First(&rec)
	id, err := rec.Value, result.Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", nil
		}
		return "", err
	}
	return id, nil
}

func (s *SqliteAdapter) LastSync() (*SyncStatus, error) {
	rec := DBRecord{}
	result := s.workingDB.Where("machine_id = ? AND key = ?", "", _last_sync_key).First(&rec)
	jDoc, err := rec.Value, result.Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return &SyncStatus{Pos: make(map[string]string)}, nil
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

// TODO do not modify oldStatus
func (s *SqliteAdapter) SaveLastSync(oldStatus *SyncStatus, newStatus *SyncStatus) error {
	if oldStatus.Pos == nil {
		oldStatus.Pos = make(map[string]string)
	}
	if newStatus.Pos == nil {
		newStatus.Pos = make(map[string]string)
	}

	for k, pos := range newStatus.Pos {
		oldStatus.Pos[k] = pos
	}
	oldStatus.Time = time.Now()
	jDoc, err := json.Marshal(oldStatus)
	if err != nil {
		return err
	}
	return s.workingDB.Where("machine_id = ? AND key = ?", "", _last_sync_key).Save(&DBRecord{Key: _last_sync_key, Value: string(jDoc), MachineID: ""}).Error
}

func (s *SqliteAdapter) Transaction(f func(s *SqliteAdapter) error) error {
	return s.workingDB.Transaction(func(tx *gorm.DB) error {
		return f(&SqliteAdapter{db: s.db, tableName: s.tableName, workingDB: tx, commitID: s.commitID, machineID: s.machineID})
	})
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

	s.db = db
	s.tableName = tableName
	// s.workingDB = db.Model(&DBRecord{}).Table(tableName)
	s.workingDB = db
	s.commitID = ""
	s.machineID = machineID
	return nil
}

func (s *SqliteAdapter) Close() error {
	if s.db != nil {
		db, err := s.db.DB()
		if err != nil {
			return err
		}
		err = db.Close()
		if err != nil {
			return err
		}
		s.db = nil
	}
	return nil
}

func withCommitID(workingDB *gorm.DB, machineID string, commitID string, f func(tx *gorm.DB) error) error {
	return workingDB.Transaction(func(tx *gorm.DB) error {
		err := f(tx)
		if err != nil {
			return err
		}
		if len(commitID) > 0 && len(machineID) > 0 {
			rec := DBRecord{}
			err := tx.Where("machine_id = ? AND key = ?", machineID, _last_commit_key).First(&rec).Error
			if err == nil {
				return tx.Where("machine_id = ? AND key = ?", machineID, _last_commit_key).Save(&DBRecord{Key: _last_commit_key, MachineID: machineID, Value: commitID}).Error
			} else if errors.Is(err, gorm.ErrRecordNotFound) {
				return tx.Create(&DBRecord{Key: _last_commit_key, MachineID: machineID, Value: commitID}).Error
			} else {
				return err
			}
		}
		return nil
	})
}

func (s *SqliteAdapter) Save(key string, value string) error {
	if len(s.machineID) == 0 {
		return fmt.Errorf("must specify a machine id")
	}

	if has, err := s.Has(key); has || err != nil {
		return withCommitID(s.workingDB, s.machineID, s.commitID, func(tx *gorm.DB) error {
			return tx.Model(&DBRecord{}).Omit("machine_id").Where("machine_id = ? AND key = ?", s.machineID, key).Updates(&DBRecord{Key: key, Value: value}).Error
		})
	} else {
		return withCommitID(s.workingDB, s.machineID, s.commitID, func(tx *gorm.DB) error {
			return s.workingDB.Create(&DBRecord{Key: key, MachineID: s.machineID, Value: value}).Error
		})
	}
}

//TODO: whether to have soft deletion enabled
//TODO: create index
func (s *SqliteAdapter) Del(key string) error {
	return withCommitID(s.workingDB, s.machineID, s.commitID, func(tx *gorm.DB) error {
		if len(s.machineID) == 0 {
			return tx.Where("key = ?", key).Delete(&DBRecord{Key: key}).Error
		} else {
			return tx.Where("machine_id = ? AND key = ?", s.machineID, key).Delete(&DBRecord{Key: key}).Error
		}
	})
}

func (s *SqliteAdapter) Has(key string) (bool, error) {
	recs := []DBRecord{}
	var result *gorm.DB
	if len(s.machineID) == 0 {
		result = s.workingDB.Find(&recs, "key = ?", key)
	} else {
		result = s.workingDB.Where("machine_id = ? AND key = ?", s.machineID, key).Find(&recs)
	}
	if result.Error != nil {
		return false, result.Error
	}

	return result.RowsAffected > 0, nil
}

func (s *SqliteAdapter) Load(key string) (string, error) {
	if len(s.machineID) == 0 {
		return "", fmt.Errorf("must specify a machine id")
	}

	rec := DBRecord{}
	result := s.workingDB.Where("machine_id = ? AND key = ?", s.machineID, key).First(&rec)
	if result.Error != nil {
		return "", result.Error
	}
	return rec.Value, nil
}

func (s *SqliteAdapter) All() ([][2]string, error) {
	records := []DBRecord{}
	var result *gorm.DB
	if len(s.machineID) > 0 {
		result = s.workingDB.Where("machine_id = ?", s.machineID).Find(&records)
	} else {
		result = s.workingDB.Find(&records)
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

func (s *SqliteAdapter) Discard(key string, gids []string) error {
	return nil
}

func _() {
	var _ Storage = &SqliteAdapter{}
}
