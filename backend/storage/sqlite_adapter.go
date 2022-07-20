package storage

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gorm.io/driver/sqlite" // Sqlite driver based on GGO

	// "github.com/glebarez/sqlite" // Pure go SQLite driver, checkout https://github.com/glebarez/sqlite for details

	"gorm.io/gorm"
)

type ChangeCount map[string]int32

// Scan implements the Scanner interface.
func (n *ChangeCount) Scan(value any) error {
	b, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("convert value to []byte failed")
	}
	return json.Unmarshal(b, n)
}

// Value implements the driver Valuer interface.
func (n ChangeCount) Value() (driver.Value, error) {
	return json.Marshal(n)
}

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
	// TODO: 日志填写changes字段
	MachineChangeCount ChangeCount
	CreatedAt          time.Time
	UpdatedAt          time.Time
	DeletedAt          sql.NullTime `gorm:"index"`
}

func (r *DBRecord) Visible() bool {
	return !r.IsDeleted && !r.IsDiscarded
}

func (r *DBRecord) Changes(machineID string) int32 {
	if r.MachineChangeCount == nil {
		return 0
	}
	if changes, ok := r.MachineChangeCount[machineID]; ok {
		return changes
	}
	return 0
}

func (r *DBRecord) AddChange(machineID string, changes int32) map[string]int32 {
	m := make(map[string]int32)
	if r.MachineChangeCount != nil {
		for k, v := range r.MachineChangeCount {
			m[k] = v
		}
	}
	if _, ok := m[machineID]; ok {
		m[machineID] += changes
	} else {
		m[machineID] = changes
	}
	return m
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
}

const _last_sync_key = "_last_sync"

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
		return f(&SqliteAdapter{db: s.db, tableName: s.tableName, workingDB: tx})
	})
}

func (s *SqliteAdapter) Init(dbFile string, tableName string) error {
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

func (s *SqliteAdapter) Save(key string, value string) error {
	if has, err := s.Has(key); has || err != nil {
		return s.workingDB.Model(&DBRecord{}).Omit("machine_id").Where("key = ?", key).Updates(&DBRecord{Key: key, Value: value}).Error
	} else {
		return s.workingDB.Create(&DBRecord{Key: key, Value: value}).Error
	}
}

//TODO: whether to have soft deletion enabled
//TODO: create index
func (s *SqliteAdapter) Del(key string) error {
	return s.workingDB.Where("key = ?", key).Delete(&DBRecord{Key: key}).Error
}

func (s *SqliteAdapter) Has(key string) (bool, error) {
	recs := []DBRecord{}
	var result *gorm.DB
	result = s.workingDB.Find(&recs, "key = ?", key)
	if result.Error != nil {
		return false, result.Error
	}

	return result.RowsAffected > 0, nil
}

func (s *SqliteAdapter) Load(key string) (string, error) {
	rec := DBRecord{}
	result := s.workingDB.Where("key = ?", key).First(&rec)
	if result.Error != nil {
		return "", result.Error
	}
	return rec.Value, nil
}

func (s *SqliteAdapter) All() ([][2]string, error) {
	records := []DBRecord{}
	var result *gorm.DB
	result = s.workingDB.Find(&records)
	if result.Error != nil {
		return nil, result.Error
	}

	kvs := [][2]string{}
	for _, rec := range records {
		if rec.Key == _last_sync_key {
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
