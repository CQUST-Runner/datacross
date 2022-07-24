package storage

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
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
	Offset        int64  `gorm:"column:offset"`
	PrevMachineID string `gorm:"column:prev_machine_id"`
	Seq           uint64 `gorm:"column:seq"`
	CurrentLogGid string `gorm:"column:gid"`
	PrevLogGid    string `gorm:"column:prev_log_gid"`
	IsDiscarded   bool   `gorm:"column:is_discarded"`
	IsDeleted     bool   `gorm:"column:is_deleted"`
	// TODO: 日志填写changes字段
	MachineChangeCount ChangeCount
	Num                int64 `gorm:"num"`
	PrevNum            int64 `gorm:"prev_num"`
	CreatedAt          time.Time
	UpdatedAt          time.Time
	DeletedAt          sql.NullTime `gorm:"index"`
}

type LogOffset struct {
	Offset    int64  `gorm:"column:offset"`
	Num       int64  `gorm:"column:num"`
	Gid       string `gorm:"column:gid"`
	MachineID string `gorm:"column:machine_id"`
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt sql.NullTime `gorm:"index"`
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
	m[machineID] += changes
	return m
}

// SqliteAdapter ...
type SqliteAdapter struct {
	db        *gorm.DB
	tableName string

	workingDB *gorm.DB
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
	err = db.AutoMigrate(&LogOffset{})
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

func (s *SqliteAdapter) updateLogOffset(offset *LogOffset) error {
	recs := []*LogOffset{}
	result := s.workingDB.Model(&LogOffset{}).Find(&recs, "machine_id = ?", offset.MachineID)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected > 0 {
		return s.workingDB.Model(&LogOffset{}).Where("machine_id = ?", offset.MachineID).Updates(offset).Error
	} else {
		return s.workingDB.Model(&LogOffset{}).Create(offset).Error
	}
}

//TODO: whether to have soft deletion enabled
//TODO: create index

func (s *SqliteAdapter) Has(gid string) (bool, error) {
	recs := []DBRecord{}
	result := s.workingDB.Model(&DBRecord{}).Find(&recs, "gid = ?", gid)
	if result.Error != nil {
		return false, result.Error
	}

	return result.RowsAffected > 0, nil
}

func (s *SqliteAdapter) Add(record *DBRecord) error {
	if has, err := s.Has(record.CurrentLogGid); has || err != nil {
		if err != nil {
			return err
		}
		return fmt.Errorf("node exists")
	} else {

		return s.Transaction(func(s2 *SqliteAdapter) error {
			if err := s2.workingDB.Model(&DBRecord{}).Create(record).Error; err != nil {
				return err
			}
			return s2.updateLogOffset(&LogOffset{MachineID: record.MachineID, Offset: record.Offset, Gid: record.CurrentLogGid, Num: record.Num})
		})

	}
}

func (s *SqliteAdapter) Replace(old string, new *DBRecord) error {
	has, err := s.Has(old)
	if err != nil {
		return err
	}
	if !has {
		return fmt.Errorf("node not exist")
	}
	return s.Transaction(func(s *SqliteAdapter) error {
		if err := s.delNode(old); err != nil {
			return err
		}
		if err := s.workingDB.Model(&DBRecord{}).Create(new).Error; err != nil {
			return err
		}
		return s.updateLogOffset(&LogOffset{MachineID: new.MachineID, Offset: new.Offset, Gid: new.CurrentLogGid, Num: new.Num})
	})
}

func (s *SqliteAdapter) delNode(gid string) error {
	return s.workingDB.Model(&DBRecord{}).Where("gid = ?", gid).Delete(&DBRecord{CurrentLogGid: gid}).Error
}

func (s *SqliteAdapter) GetByKey(key string) ([]*DBRecord, error) {

	records := []*DBRecord{}
	result := s.workingDB.Model(&DBRecord{}).Where("key = ?", key).Find(&records)
	if result.Error != nil {
		return nil, result.Error
	}
	return records, nil
}

func (s *SqliteAdapter) GetByGid(gid string) (*DBRecord, error) {
	rec := DBRecord{}
	result := s.workingDB.Model(&DBRecord{}).Where("gid = ?", gid).First(&rec)
	if result.Error != nil {
		return nil, result.Error
	}
	return &rec, nil
}

func (s *SqliteAdapter) AllNodes() ([]*DBRecord, error) {
	records := []*DBRecord{}
	result := s.workingDB.Model(&DBRecord{}).Find(&records)
	if result.Error != nil {
		return nil, result.Error
	}
	return records, nil
}

func (s *SqliteAdapter) Offsets() (map[string]*LogOffset, error) {

	records := []*LogOffset{}
	result := s.workingDB.Model(&LogOffset{}).Find(&records)
	if result.Error != nil {
		return nil, result.Error
	}
	offsets := make(map[string]*LogOffset)
	for _, rec := range records {
		offsets[rec.MachineID] = rec
	}
	return offsets, nil
}

func (s *SqliteAdapter) Merge(other ReadOnlyNodeStorage) error {
	return fmt.Errorf("unsupported")
}

func _() {
	var _ NodeStorage = &SqliteAdapter{}
}
