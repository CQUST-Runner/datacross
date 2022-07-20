package storage

import "fmt"

// // HybridStorage ...
// type HybridStorage struct {
// 	m      *MapWithWal
// 	sqlite *SqliteAdapter
// }

// func (s *HybridStorage) keepUpWithLog() bool {
// 	// TODO: cache them?
// 	lastEntryID := s.m.LastEntryID()
// 	lastCommit, err := s.sqlite.LastCommit()
// 	if err != nil {
// 		logger.Error("get last commit failed[%v]", err)
// 		return false
// 	}
// 	if lastEntryID != lastCommit {
// 		if len(lastCommit) == 0 {
// 			// make it explicitly start from beginning
// 			err = s.m.log.Replay(s.sqlite, "")
// 		} else {
// 			err = s.m.log.Replay(s.sqlite, lastCommit)
// 		}
// 		if err != nil {
// 			logger.Error("replay failed[%v]", err)
// 			return false
// 		}
// 	}
// 	lastCommit, err = s.sqlite.LastCommit()
// 	if err != nil {
// 		logger.Error("get last commit again failed[%v]", err)
// 		return false
// 	}
// 	logger.Info("last entry id[%v], last commit[%v]", lastEntryID, lastCommit)
// 	return lastCommit == lastEntryID
// }

// func (s *HybridStorage) Init(m *MapWithWal, sqlite *SqliteAdapter) error {
// 	s.m = m
// 	s.sqlite = sqlite

// 	ok := s.keepUpWithLog()
// 	if !ok {
// 		logger.Warn("sqlite is lag behind from wal")
// 	}

// 	return nil
// }

// func (s *HybridStorage) Close() {
// 	if s.m != nil {
// 		err := s.m.Close()
// 		if err != nil {
// 			logger.Error("close map failed[%v]", err)
// 		}
// 		s.m = nil
// 	}
// 	if s.sqlite != nil {
// 		err := s.sqlite.Close()
// 		if err != nil {
// 			logger.Error("close sqlite failed[%v]", err)
// 		}
// 		s.sqlite = nil
// 	}
// }

// func (s *HybridStorage) WithCommitID(string) Storage {
// 	return s
// }

// func (s *HybridStorage) WithMachineID(string) Storage {
// 	return s
// }

// func (s *HybridStorage) Save(key string, value string) error {
// 	ok := s.keepUpWithLog()
// 	if !ok {
// 		logger.Warn("sqlite is lag behind from wal")
// 	}

// 	var commitID string
// 	err := s.m.Retrieve(&commitID).Save(key, value)
// 	if err != nil {
// 		return err
// 	}

// 	if !ok {
// 		return nil
// 	}
// 	err = s.sqlite.WithCommitID(commitID).Save(key, value)
// 	if err != nil {
// 		logger.Warn("save key[%v] value[%v] into sqlite failed", key, value)
// 	}
// 	return nil
// }

// func (s *HybridStorage) Del(key string) error {
// 	ok := s.keepUpWithLog()
// 	if !ok {
// 		logger.Warn("sqlite is lag behind from wal")
// 	}

// 	var commitID string
// 	err := s.m.Retrieve(&commitID).Del(key)
// 	if err != nil {
// 		return err
// 	}

// 	if !ok {
// 		return nil
// 	}
// 	err = s.sqlite.WithCommitID(commitID).Del(key)
// 	if err != nil {
// 		logger.Warn("del key[%v] from sqlite failed", key)
// 	}

// 	return nil
// }

// func (s *HybridStorage) Has(key string) (bool, error) {
// 	has, err := s.m.Has(key)
// 	if err != nil {
// 		return false, err
// 	}
// 	if has {
// 		return true, nil
// 	}

// 	return s.sqlite.Has(key)
// }

// func (s *HybridStorage) Load(key string) (string, error) {
// 	val, err := s.m.Load(key)
// 	if err == nil {
// 		return val, nil
// 	}
// 	return s.sqlite.Load(key)
// }

// func (s *HybridStorage) All() ([][2]string, error) {
// 	err := s.m.Merge(s.sqlite)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return s.m.All()
// }

// func (s *HybridStorage) Merge(Storage) error {
// 	return fmt.Errorf("unsupported")
// }

// func (s *HybridStorage) Discard(key string, gids []string) error {
// 	return fmt.Errorf("unsupported")
// }

// func _() {
// 	var _ Storage = &HybridStorage{}
// }

// HybridStorage ...
type HybridStorage struct {
	f         GrowOnlyForest
	machineID string
	w         *Wal
}

func (s *HybridStorage) Init(w *Wal) error {
	n := NodeStorageImpl{}
	n.Init()
	f := GrowOnlyForestImpl{}
	f.Init(&n)

	s.w = w
	s.f = &f
	return nil
}

func (s *HybridStorage) Close() {
}

func (s *HybridStorage) WithCommitID(string) Storage {
	return s
}

func (s *HybridStorage) WithMachineID(string) Storage {
	return s
}

// suppose Visible()==true
func compareNode(a *DBRecord, b *DBRecord, machineID string) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return 1
	}
	if b == nil {
		return -1
	}
	if a.Changes(machineID) > b.Changes(machineID) {
		return 1
	} else if a.Changes(machineID) < b.Changes(machineID) {
		return -1
	} else {
		if a.MachineID > b.MachineID {
			return 1
		} else if a.MachineID < b.MachineID {
			return -1
		} else {
			// should not be
			return 0
		}
	}
}

func findMain(a []*DBRecord, machineID string) *DBRecord {
	var maxRecord *DBRecord
	for _, record := range a {
		if compareNode(record, maxRecord, machineID) > 0 {
			maxRecord = record
		}
	}
	return maxRecord
}

func (s *HybridStorage) Save(key string, value string) error {
	leaves, err := s.f.GetLeavesByKey(key)
	if err != nil {
		return err
	}

	if len(leaves) == 0 {
		gid, err := s.w.Append(&LogOperation{
			Op:            int32(Op_Modify),
			Key:           key,
			Value:         value,
			Gid:           "",
			PrevGid:       "",
			PrevValue:     "",
			Seq:           0,
			MachineId:     s.machineID,
			PrevMachineId: "",
		})
		if err != nil {
			return err
		}

		return s.f.AddLeaf(&DBRecord{
			Key:                key,
			Value:              value,
			MachineID:          s.machineID,
			PrevMachineID:      "",
			Seq:                0,
			CurrentLogGid:      gid,
			PrevLogGid:         "",
			IsDiscarded:        false,
			IsDeleted:          false,
			MachineChangeCount: map[string]int32{s.machineID: 1},
		}, false)
	}

	main := findMain(leaves, s.machineID)
	if main == nil {
		return fmt.Errorf("cannot find main node")
	}

	gid, err := s.w.Append(&LogOperation{
		Op:            int32(Op_Modify),
		Key:           key,
		Value:         value,
		Gid:           "",
		PrevGid:       main.CurrentLogGid,
		PrevValue:     main.Value,
		Seq:           main.Seq + 1,
		MachineId:     s.machineID,
		PrevMachineId: main.MachineID,
	})
	if err != nil {
		return err
	}
	return s.f.AddLeaf(&DBRecord{
		Key:                key,
		Value:              value,
		MachineID:          s.machineID,
		PrevMachineID:      main.PrevMachineID,
		Seq:                main.Seq + 1,
		CurrentLogGid:      gid,
		PrevLogGid:         main.CurrentLogGid,
		IsDiscarded:        false,
		IsDeleted:          false,
		MachineChangeCount: main.AddChange(s.machineID, 1),
	}, false)
}

func (s *HybridStorage) Del(key string) error {
	leaves, err := s.f.GetLeavesByKey(key)
	if err != nil {
		return err
	}
	if len(leaves) == 0 {
		return nil
	}
	main := findMain(leaves, s.machineID)
	if main == nil {
		return fmt.Errorf("cannot find main node")
	}

	gid, err := s.w.Append(&LogOperation{
		Op:            int32(Op_Del),
		Key:           key,
		Gid:           "",
		PrevGid:       main.CurrentLogGid,
		PrevValue:     main.Value,
		Seq:           main.Seq + 1,
		MachineId:     s.machineID,
		PrevMachineId: main.MachineID,
	})
	if err != nil {
		return err
	}
	return s.f.AddLeaf(&DBRecord{
		Key:                key,
		MachineID:          s.machineID,
		PrevMachineID:      main.PrevMachineID,
		Seq:                main.Seq + 1,
		CurrentLogGid:      gid,
		PrevLogGid:         main.CurrentLogGid,
		IsDiscarded:        false,
		IsDeleted:          true,
		MachineChangeCount: main.AddChange(s.machineID, 1),
	}, false)
}

func (s *HybridStorage) Has(key string) (bool, error) {
	leaves, err := s.f.GetLeavesByKey(key)
	if err != nil {
		return false, err
	}
	return findMain(leaves, s.machineID) != nil, nil
}

func (s *HybridStorage) Load(key string) (string, error) {
	leaves, err := s.f.GetLeavesByKey(key)
	if err != nil {
		return "", err
	}
	if len(leaves) == 0 {
		return "", fmt.Errorf("not exist")
	}

	main := findMain(leaves, s.machineID)
	if main == nil {
		return "", fmt.Errorf("cannot find main node")
	}
	return main.Value, nil
}

func (s *HybridStorage) All() ([][2]string, error) {
	leaves, err := s.f.AllLeaves()
	if err != nil {
		return nil, err
	}

	m := make(map[string]*DBRecord)
	for _, l := range leaves {
		if l == nil || !l.Visible() {
			continue
		}
		e, ok := m[l.Key]
		if !ok {
			m[l.Key] = e

		} else {
			if compareNode(l, e, s.machineID) > 0 {
				m[l.Key] = e
			}
		}

	}

	results := make([][2]string, 0)
	for _, l := range m {
		results = append(results, [2]string{l.Key, l.Value})
	}
	return results, nil
}

func (s *HybridStorage) Merge(Storage) error {
	return fmt.Errorf("unsupported")
}

func (s *HybridStorage) Discard(key string, gids []string) error {
	return fmt.Errorf("unsupported")
}

func _() {
	var _ Storage = &HybridStorage{}
}
