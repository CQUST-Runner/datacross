package storage

import "fmt"

type GrowOnlyForest interface {
	AddLeaf(record *DBRecord, force bool) error
	AllLeaves() ([]*DBRecord, error)
	GetLeavesByKey(key string) ([]*DBRecord, error)
}

type GrowOnlyForestImpl struct {
	s NodeStorage
}

func (f *GrowOnlyForestImpl) Init(s NodeStorage) {
	f.s = s
}

func (f *GrowOnlyForestImpl) AddLeaf(record *DBRecord, force bool) error {
	tmp, err := f.s.GetByGid(record.CurrentLogGid)
	if err != nil {
		return err
	}
	if tmp != nil {
		return fmt.Errorf("node exists")
	}

	if record.Seq == 0 {
		return f.s.Add(&DBRecord{
			Key:           record.Key,
			Value:         record.Value,
			MachineID:     record.MachineID,
			Seq:           record.Seq,
			CurrentLogGid: record.CurrentLogGid,
			IsDiscarded:   record.IsDiscarded,
			IsDeleted:     record.IsDeleted,
			IsMain:        record.IsMain,
		})
	}

	parent, err := f.s.GetByGid(record.PrevLogGid)
	if err != nil {
		return err
	}
	if parent != nil {
		// only store the leaves
		return f.s.Replace(parent.CurrentLogGid, &DBRecord{
			Key:           record.Key,
			Value:         record.Value,
			MachineID:     record.MachineID,
			PrevMachineID: parent.MachineID,
			Seq:           parent.Seq + 1,
			CurrentLogGid: record.CurrentLogGid,
			PrevLogGid:    parent.CurrentLogGid,
			IsDiscarded:   record.IsDiscarded,
			IsDeleted:     record.IsDeleted,
			IsMain:        record.IsMain,
		})
	} else {
		if force {
			// we'll allow it if it is required to do so
			return f.s.Add(&DBRecord{
				Key:           record.Key,
				Value:         record.Value,
				MachineID:     record.MachineID,
				Seq:           record.Seq,
				CurrentLogGid: record.CurrentLogGid,
				IsDiscarded:   record.IsDiscarded,
				IsDeleted:     record.IsDeleted,
				IsMain:        record.IsMain,
			})
		} else {
			return fmt.Errorf("cannot find parent node")
		}
	}
}

func (f *GrowOnlyForestImpl) AllLeaves() ([]*DBRecord, error) {
	return f.s.AllNodes()
}

func (f *GrowOnlyForestImpl) GetLeavesByKey(key string) ([]*DBRecord, error) {
	return f.s.GetByKey(key)
}

func init() {
	var _ GrowOnlyForest = &GrowOnlyForestImpl{}
}
