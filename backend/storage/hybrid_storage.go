package storage

import "fmt"

// HybridStorage ...
type HybridStorage struct {
	f         NodeStorage
	machineID string
	w         *Wal
}

func (s *HybridStorage) Init(w *Wal, machineID string) error {
	f := NodeStorageImpl{}
	f.Init()

	r := LogRunner{}
	if err := r.Init(machineID, &f); err != nil {
		return err
	}

	result, err := r.Run(&LogInput{w: w, machineID: machineID, start: "", startNum: 0})
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
	}

	if a.Seq > b.Seq {
		return 1
	} else if a.Seq < b.Seq {
		return -1
	}

	if a.MachineID > b.MachineID {
		return 1
	} else if a.MachineID < b.MachineID {
		return -1
	} else {
		// should not be
		return 0
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
	leaves, err := s.f.GetByKey(key)
	if err != nil {
		return err
	}
	leaves = filterVisible(leaves)

	if len(leaves) == 0 {
		gid, num, err := s.w.Append(&LogOperation{
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
			PrevNum:       0,
		})
		if err != nil {
			return err
		}

		return s.f.Add(&DBRecord{
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
			Num:                num,
			PrevNum:            0,
		})
	}

	main := findMain(leaves, s.machineID)
	if main == nil {
		return fmt.Errorf("cannot find main node")
	}

	gid, num, err := s.w.Append(&LogOperation{
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
		PrevNum:       main.Num,
	})
	if err != nil {
		return err
	}
	return s.f.Replace(main.CurrentLogGid, &DBRecord{
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
		Num:                num,
		PrevNum:            main.Num,
	})
}

func (s *HybridStorage) Del(key string) error {
	leaves, err := s.f.GetByKey(key)
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

	gid, num, err := s.w.Append(&LogOperation{
		Op:            int32(Op_Del),
		Key:           key,
		Gid:           "",
		PrevGid:       main.CurrentLogGid,
		PrevValue:     main.Value,
		Seq:           main.Seq + 1,
		MachineId:     s.machineID,
		PrevMachineId: main.MachineID,
		Changes:       main.AddChange(s.machineID, 1),
		PrevNum:       main.Num,
	})
	if err != nil {
		return err
	}
	return s.f.Replace(main.CurrentLogGid, &DBRecord{
		Key:                key,
		MachineID:          s.machineID,
		PrevMachineID:      main.PrevMachineID,
		Seq:                main.Seq + 1,
		CurrentLogGid:      gid,
		PrevLogGid:         main.CurrentLogGid,
		IsDiscarded:        false,
		IsDeleted:          true,
		MachineChangeCount: main.AddChange(s.machineID, 1),
		Num:                num,
		PrevNum:            main.Num,
	})
}

func (s *HybridStorage) Has(key string) (bool, error) {
	leaves, err := s.f.GetByKey(key)
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

func (s *HybridStorage) Load(key string) (*Value, error) {
	leaves, err := s.f.GetByKey(key)
	if err != nil {
		return nil, err
	}
	leaves = filterVisible(leaves)
	if len(leaves) == 0 {
		return nil, fmt.Errorf("not exist")
	}

	main := findMain(leaves, s.machineID)
	if main == nil {
		return nil, fmt.Errorf("cannot find main node")
	}
	v := Value{key: key, value: main.Value, machineID: main.MachineID,
		gid: main.CurrentLogGid, seq: 0}
	seq := 1
	for _, e := range leaves {
		if e == nil {
			continue
		}
		if e.CurrentLogGid == main.CurrentLogGid {
			continue
		}
		v.branches = append(v.branches, &Value{key: key, value: e.Value, machineID: e.MachineID, gid: e.CurrentLogGid, seq: seq})
		seq++
	}
	return &v, nil
}

func (s *HybridStorage) All() ([]*Value, error) {
	leaves, err := s.f.AllNodes()
	if err != nil {
		return nil, err
	}

	m := make(map[string][]*DBRecord)
	for _, l := range leaves {
		if l == nil || !l.Visible() {
			continue
		}
		e, ok := m[l.Key]
		if !ok {
			m[l.Key] = append(m[l.Key], l)
		} else {
			e0 := e[0]
			if compareNode(l, e0, s.machineID) > 0 {
				m[l.Key][0] = l
				m[l.Key] = append(m[l.Key], e0)
			} else {
				m[l.Key] = append(m[l.Key], l)
			}
		}
	}

	results := make([]*Value, 0)
	for _, l := range m {
		var v *Value
		seq := 1
		for i, e := range l {
			if e == nil {
				continue
			}
			if i == 0 {
				v = &Value{key: e.Key, value: e.Value, machineID: e.MachineID, gid: e.CurrentLogGid, seq: 0}
			} else {
				v.branches = append(v.branches, &Value{
					key: e.Key, value: e.Value,
					machineID: e.MachineID, gid: e.CurrentLogGid,
					seq: seq,
				})
				seq++
			}
		}
		results = append(results, v)
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
