package storage

import "fmt"

// HybridStorage ...
type HybridStorage struct {
	f         GrowOnlyForest
	machineID string
	w         *Wal
}

func (s *HybridStorage) Init(w *Wal, machineID string) error {
	n := NodeStorageImpl{}
	n.Init()
	f := GrowOnlyForestImpl{}
	f.Init(&n)

	r := LogRunner{}
	if err := r.Init(machineID, &n); err != nil {
		return err
	}

	result, err := r.Run(&LogInput{w: w, machineID: machineID, start: ""})
	if err != nil {
		return err
	}
	_ = result

	s.machineID = machineID
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
		return -1
	}
	if b == nil {
		return 1
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
	leaves = filterVisible(leaves)

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
			Changes:       map[string]int32{s.machineID: 1},
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
		Changes:       main.AddChange(s.machineID, 1),
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
	leaves = filterVisible(leaves)
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
		Changes:       main.AddChange(s.machineID, 1),
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
	leaves = filterVisible(leaves)

	return findMain(leaves, s.machineID) != nil, nil
}

func filterVisible(a []*DBRecord) []*DBRecord {
	results := make([]*DBRecord, 0, len(a))
	for _, record := range a {
		if record != nil && record.Visible() {
			results = append(results, record)
		}
	}
	return results
}

func (s *HybridStorage) Load(key string) (string, error) {
	leaves, err := s.f.GetLeavesByKey(key)
	if err != nil {
		return "", err
	}
	leaves = filterVisible(leaves)
	if len(leaves) == 0 {
		return "", fmt.Errorf("not exist")
	}

	main := findMain(leaves, s.machineID)
	if main == nil {
		return "", fmt.Errorf("cannot find main node")
	}
	return main.Value, nil
}

// TODO findMain有问题
// TODO run log时本机引用其他机器的日志
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
			m[l.Key] = l
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
