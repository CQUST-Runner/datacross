package storage

import (
	"fmt"
)

type LogProcess struct {
	offset int64
	gid    string
	num    int64
}

type LogProcessMgr struct {
	m map[string]*LogProcess
}

func (m *LogProcessMgr) Init() {
	m.m = make(map[string]*LogProcess)
}

func (m *LogProcessMgr) Get(machineID string) *LogProcess {
	if p, ok := m.m[machineID]; ok {
		return p
	}
	p := LogProcess{}
	m.m[machineID] = &p
	return &p
}

func (m *LogProcessMgr) Set(machineID string, process *LogProcess) {
	m.m[machineID] = process
}

// HybridStorage ...
type HybridStorage struct {
	network *NetworkInfo2
	m       *LogProcessMgr
	me      *ParticipantInfo

	w         *WalHelper
	f         NodeStorage
	machineID string
}

func (s *HybridStorage) runLogInputs() (inputs []*LogInput, retErr error) {
	inputs = []*LogInput{}
	for _, p := range s.network.participants {
		var process LogProcess = *s.m.Get(p.name)
		w := Wal{}
		err := w.Init(p.walFile, &BinLog{}, true)
		if err != nil {
			return nil, err
		}
		defer func() {
			if retErr != nil {
				err := w.Close()
				if err != nil {
					logger.Error("close wal file failed", err)
				}
			}
		}()

		inputs = append(inputs, &LogInput{
			machineID: p.name, w: &w,
			process: &process})
	}
	return inputs, nil
}

func (s *HybridStorage) runLog() error {
	runner := LogRunner{}
	err := runner.Init(s.machineID, s.f)
	if err != nil {
		return err
	}

	inputs, err := s.runLogInputs()
	if err != nil {
		return err
	}
	// TODO inputs.Close

	results, err := runner.Run(inputs...)
	if err != nil {
		return err
	}

	for machineID, process := range results.status {
		s.m.Set(machineID, process)
	}

	return nil
}

func (s *HybridStorage) Init(wd string, machineID string) error {

	network := NetworkInfo2{}
	err := network.Init(wd)
	if err != nil {
		return err
	}

	me := network.Add(machineID)

	m := LogProcessMgr{}
	m.Init()

	ns := NodeStorageImpl{}
	ns.Init()

	w := WalHelper{}
	w.Init(me.walFile, &BinLog{}, 1)

	s.network = &network
	s.m = &m
	s.f = &ns
	s.machineID = machineID
	s.w = &w
	s.me = me

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

func (s *HybridStorage) Save(key string, value string) error {
	if err := s.runLog(); err != nil {
		return err
	}

	leaves, err := s.f.GetByKey(key)
	if err != nil {
		return err
	}
	leaves = filterVisible(leaves)

	if len(leaves) == 0 {
		_, _, err := s.w.Append(&LogOperation{
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

		return nil
	}

	main := findMain(leaves, s.machineID)
	if main == nil {
		return fmt.Errorf("cannot find main node")
	}

	_, _, err = s.w.Append(&LogOperation{
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
	return nil
}

func (s *HybridStorage) Del(key string) error {

	if err := s.runLog(); err != nil {
		return err
	}

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

	_, _, err = s.w.Append(&LogOperation{
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

	return nil
}

func (s *HybridStorage) Has(key string) (bool, error) {

	if err := s.runLog(); err != nil {
		return false, err
	}

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

	if err := s.runLog(); err != nil {
		return nil, err
	}

	leaves, err := s.f.GetByKey(key)
	if err != nil {
		return nil, err
	}
	leaves = filterVisible(leaves)
	if len(leaves) == 0 {
		return nil, fmt.Errorf("not exist")
	}

	v := Value{}
	err = v.from(leaves, s.machineID)
	if err != nil {
		return nil, err
	}
	return &v, nil
}

func (s *HybridStorage) All() ([]*Value, error) {

	if err := s.runLog(); err != nil {
		return nil, err
	}

	leaves, err := s.f.AllNodes()
	if err != nil {
		return nil, err
	}

	m := make(map[string][]*DBRecord)
	for _, l := range leaves {
		if l == nil || !l.Visible() {
			continue
		}
		m[l.Key] = append(m[l.Key], l)
	}

	results := make([]*Value, 0)
	for _, l := range m {
		v := Value{}
		err := v.from(l, s.machineID)
		if err != nil {
			return nil, err
		}
		results = append(results, &v)
	}
	return results, nil
}

func (s *HybridStorage) discard(gid string) error {
	record, err := s.f.GetByGid(gid)
	if err != nil {
		return err
	}

	_, _, err = s.w.Append(&LogOperation{
		Op:            int32(Op_Discard),
		Key:           record.Key,
		PrevGid:       record.CurrentLogGid,
		PrevValue:     record.Value,
		Seq:           record.Seq + 1,
		MachineId:     s.machineID,
		PrevMachineId: record.MachineID,
		Changes:       record.AddChange(s.machineID, 1),
		PrevNum:       record.Num,
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *HybridStorage) Accept(v *Value, seq int) error {

	if err := s.runLog(); err != nil {
		return err
	}

	if len(v.Branches()) == 0 {
		return fmt.Errorf("key is not in conflict state")
	}
	if !v.ValidSeq(seq) {
		return fmt.Errorf("seq is invalid")
	}

	for _, version := range v.versions {
		if version == nil {
			continue
		}
		if version.seq != seq {
			err := s.discard(version.gid)
			if err != nil {
				logger.Warn("discard version failed, seq[%v] gid[%v]", version.seq, version.gid)
			}
		}
	}
	return nil
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
