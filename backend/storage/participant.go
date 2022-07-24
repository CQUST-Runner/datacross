package storage

import (
	"fmt"
	"os"
	"path"
	"time"
)

type ParticipantInfo struct {
	name string

	personalPath string
	walFile      string
	dbFile       string

	network *NetworkInfo
}

func (p *ParticipantInfo) Init(wd string, name string, n *NetworkInfo) error {
	personalPath := getPersonalPath(wd, name)
	walPath := getWalFilePath(personalPath)
	dbPath := getDBFilePath(personalPath)

	if !IsDir(personalPath) {
		err := os.MkdirAll(personalPath, 0777)
		if err != nil {
			return err
		}
	}

	if !IsFile(walPath) {
		w := Wal{}
		err := w.Init(walPath, &BinLog{}, false)
		if err != nil {
			return err
		}
		err = w.Close()
		if err != nil {
			return err
		}
	}

	p.name = name
	p.personalPath = personalPath
	p.walFile = walPath
	p.dbFile = dbPath
	p.network = n
	return nil
}

type NetworkInfo struct {
	wd           string
	participants map[string]*ParticipantInfo
}

func (n *NetworkInfo) Init(wd string) error {
	all, err := discoveryAllParticipants(wd)
	if err != nil {
		return err
	}
	participants := map[string]*ParticipantInfo{}
	for _, name := range all {
		p := ParticipantInfo{}
		_ = p.Init(wd, name, n)
		participants[name] = &p
	}

	n.wd = wd
	n.participants = participants

	return nil
}

func (n *NetworkInfo) Add(name string) *ParticipantInfo {
	if _, ok := n.participants[name]; !ok {
		p := ParticipantInfo{}
		// TODO: handle error
		_ = p.Init(n.wd, name, n)
		n.participants[name] = &p

	}
	return n.participants[name]
}

const WalFileName = "0.wal"
const DBFileName = "0.db"
const SyncInterval = time.Minute

func discoveryAllParticipants(wd string) ([]string, error) {
	entries, err := os.ReadDir(wd)
	if err != nil {
		return nil, err
	}
	list := []string{}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		sign := path.Join(path.Join(wd, e.Name()), WalFileName)
		if IsFile(sign) {
			list = append(list, e.Name())
		}
	}
	return list, nil
}

func getPersonalPath(wd string, pname string) string {
	personalPath := path.Join(wd, pname)
	return personalPath
}

func getWalFilePath(personalPath string) string {
	walFile := path.Join(personalPath, WalFileName)
	return walFile
}

func getDBFilePath(personalPath string) string {
	dbFile := path.Join(personalPath, DBFileName)
	return dbFile
}

// TODO log format converter
// TODO command line db tool
// TODO background syncing, thread safety?
// TODO set json flag to output single line json

type LogProcessMgr struct {
	m map[string]*LogProcess
}

func (m *LogProcessMgr) Init(process ...*LogProcess) {
	m.m = make(map[string]*LogProcess)
	for _, p := range process {
		if p == nil {
			continue
		}
		m.Set(p.MachineID, p)
	}
}

func (m *LogProcessMgr) Get(machineID string) *LogProcess {
	if p, ok := m.m[machineID]; ok {
		return p
	}
	p := LogProcess{Offset: HeaderSize}
	m.m[machineID] = &p
	return &p
}

func (m *LogProcessMgr) Set(machineID string, process *LogProcess) {
	m.m[machineID] = process
}

// Participant ...
type Participant struct {
	network *NetworkInfo
	m       *LogProcessMgr
	me      *ParticipantInfo

	w         *WalHelper
	f         ReadOnlyNodeStorage
	runner    *LogRunner
	machineID string

	// TODO
	lastSyncTime time.Time
}

func runLogInputs(network *NetworkInfo, m *LogProcessMgr) (inputs []*LogInput, retErr error) {
	inputs = []*LogInput{}
	for _, p := range network.participants {
		var process LogProcess = *m.Get(p.name)
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

func runLog(runner *LogRunner, network *NetworkInfo, m *LogProcessMgr) error {

	inputs, err := runLogInputs(network, m)
	if err != nil {
		return err
	}
	// TODO inputs.Close

	results, err := runner.Run(inputs...)
	if err != nil {
		return err
	}

	for machineID, process := range results.status {
		m.Set(machineID, process)
	}

	return nil
}

func (s *Participant) newNodeStorageFromSqlite(dbFile string) (NodeStorage, []*LogProcess, error) {

	ns := NodeStorageImpl{}
	ns.Init()

	sqlite := SqliteAdapter{}
	err := sqlite.Init(dbFile, "")
	if err != nil {
		return nil, nil, err
	}
	defer sqlite.Close()

	offsets, err := sqlite.Processes()
	if err != nil {
		return nil, nil, err
	}

	err = ns.Merge(&sqlite)
	if err != nil {
		return nil, nil, err
	}

	return &ns, offsets, nil
}

func (s *Participant) runLogTillEnd() error {
	if err := runLog(s.runner, s.network, s.m); err != nil {
		return err
	}
	offset, err := s.w.Offset()
	if err != nil {
		return err
	}
	if s.m.Get(s.machineID).Offset != offset {
		return fmt.Errorf("log process is not yet end")
	}
	return nil
}

func (s *Participant) Init(wd string, machineID string) error {
	if !path.IsAbs(wd) && !(len(wd) > 1 && wd[1] == ':') {
		cwd, err := os.Getwd()
		if err != nil {
			return err
		}
		wd = path.Join(cwd, wd)
	}
	wd = path.Clean(wd)
	if !IsDir(wd) {
		err := os.MkdirAll(wd, 0777)
		if err != nil {
			return err
		}
	}

	network := NetworkInfo{}
	err := network.Init(wd)
	if err != nil {
		return err
	}

	me := network.Add(machineID)

	ns, offsets, err := s.newNodeStorageFromSqlite(me.dbFile)
	if err != nil {
		return err
	}

	m := LogProcessMgr{}
	m.Init(offsets...)

	runner := LogRunner{}
	err = runner.Init(machineID, ns)
	if err != nil {
		return err
	}

	w := WalHelper{}
	w.Init(me.walFile, &BinLog{}, 1)

	s.network = &network
	s.m = &m
	s.f = ns
	s.machineID = machineID
	s.w = &w
	s.me = me
	s.runner = &runner
	return nil
}

func (s *Participant) persistToSqlite() error {
	sqlite := SqliteAdapter{}
	err := sqlite.Init(s.me.dbFile, "")
	if err != nil {
		return err
	}
	defer func() {
		err = sqlite.Close()
		if err != nil {
			logger.Error("close sqlite failed[%v]", err)
		}
	}()

	processes, err := sqlite.Processes()
	if err != nil {
		return err
	}
	m := LogProcessMgr{}
	m.Init(processes...)

	runner := LogRunner{}
	err = runner.Init(s.machineID, &sqlite)
	if err != nil {
		return err
	}
	return runLog(&runner, s.network, &m)
}

func (s *Participant) Close() {
	logger.Info("persist to sqlite...")
	err := s.persistToSqlite()
	if err != nil {
		logger.Error("persist to sqlite failed[%v]", err)
	}
}

func (s *Participant) WithCommitID(string) Storage {
	return s
}

func (s *Participant) WithMachineID(string) Storage {
	return s
}

func (s *Participant) Save(key string, value string) error {
	if err := s.runLogTillEnd(); err != nil {
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

func (s *Participant) Del(key string) error {

	if err := s.runLogTillEnd(); err != nil {
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

func (s *Participant) Has(key string) (bool, error) {

	if err := s.runLogTillEnd(); err != nil {
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

func (s *Participant) Load(key string) (*Value, error) {

	if err := s.runLogTillEnd(); err != nil {
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

func (s *Participant) All() ([]*Value, error) {

	if err := s.runLogTillEnd(); err != nil {
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

func (s *Participant) discard(gid string) (*LogOperation, error) {
	record, err := s.f.GetByGid(gid)
	if err != nil {
		return nil, err
	}

	return &LogOperation{
		Op:            int32(Op_Discard),
		Key:           record.Key,
		PrevGid:       record.CurrentLogGid,
		PrevValue:     record.Value,
		Seq:           record.Seq + 1,
		MachineId:     s.machineID,
		PrevMachineId: record.MachineID,
		Changes:       record.AddChange(s.machineID, 1),
		PrevNum:       record.Num,
	}, nil
}

func (s *Participant) Accept(v *Value, seq int) error {

	if err := s.runLogTillEnd(); err != nil {
		return err
	}

	if len(v.Branches()) == 0 {
		return fmt.Errorf("key is not in conflict state")
	}
	if !v.ValidSeq(seq) {
		return fmt.Errorf("seq is invalid")
	}

	operations := []*LogOperation{}
	for _, version := range v.versions {
		if version == nil {
			continue
		}
		if version.seq != seq {
			op, err := s.discard(version.gid)
			if err != nil {
				logger.Warn("discard version failed, seq[%v] gid[%v]", version.seq, version.gid)
				return err
			}
			operations = append(operations, op)
		}
	}
	if len(operations) == 0 {
		return nil
	}

	_, _, err := s.w.Append(operations...)
	return err
}

func (s *Participant) AllConflicts() ([]*Value, error) {
	all, err := s.All()
	if err != nil {
		return nil, err
	}
	results := []*Value{}
	for _, v := range all {
		if v == nil {
			continue
		}
		if len(v.Branches()) > 0 {
			results = append(results, v)
		}
	}
	return results, nil
}

func (s *Participant) Merge(Storage) error {
	return fmt.Errorf("unsupported")
}

func _() {
	var _ Storage = &Participant{}
}
