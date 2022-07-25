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

func initParticipant(wd string, name string, network *NetworkInfo) (*ParticipantInfo, error) {
	p := ParticipantInfo{}
	p.Init(wd, name, network)

	if !IsDir(p.personalPath) {
		err := os.MkdirAll(p.personalPath, 0777)
		if err != nil {
			return nil, err
		}
	}

	if !IsFile(p.walFile) {
		w := Wal{}
		err := w.Init(p.walFile, &BinLog{}, false)
		if err != nil {
			return nil, err
		}
		err = w.Close()
		if err != nil {
			return nil, err
		}
	}

	return &p, nil
}

func (p *ParticipantInfo) Init(wd string, name string, n *NetworkInfo) {
	personalPath := getPersonalPath(wd, name)
	walPath := getWalFilePath(personalPath)
	dbPath := getDBFilePath(personalPath)

	p.name = name
	p.personalPath = personalPath
	p.walFile = walPath
	p.dbFile = dbPath
	p.network = n
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
		p.Init(wd, name, n)
		participants[name] = &p
	}

	n.wd = wd
	n.participants = participants
	return nil
}

func (n *NetworkInfo) Add(name string) *ParticipantInfo {
	if _, ok := n.participants[name]; !ok {
		p := ParticipantInfo{}
		p.Init(n.wd, name, n)
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

	w      *WalHelper
	ns     ReadOnlyNodeStorage
	runner *LogRunner

	// TODO
	lastSyncTime time.Time
}

func makeRunLogInputs(network *NetworkInfo, m *LogProcessMgr) (inputs []*LogInput, retErr error) {
	inputs = []*LogInput{}
	for _, p := range network.participants {
		var process LogProcess = *m.Get(p.name)
		w := Wal{}
		err := w.Init(p.walFile, &BinLog{}, true)
		if err != nil {
			return nil, err
		}
		defer func(w *Wal, filename string) {
			if retErr != nil {
				err := w.Close()
				if err != nil {
					logger.Error("close wal file[%v] failed[%v]", filename, err)
				}
			}
		}(&w, p.walFile)

		inputs = append(inputs, &LogInput{
			machineID: p.name, w: &w,
			process: &process})
	}
	return inputs, nil
}

func closeRunLogInputs(inputs ...*LogInput) {
	for _, input := range inputs {
		if input == nil {
			continue
		}
		var filename string
		if input.w.f != nil {
			filename = input.w.f.Path()
		}
		err := input.w.Close()
		if err != nil {
			logger.Error("close wal file[%v] failed[%v]", filename, err)
		}
	}
}

func runLog(runner *LogRunner, network *NetworkInfo, m *LogProcessMgr) error {
	inputs, err := makeRunLogInputs(network, m)
	if err != nil {
		return err
	}
	defer func(inputs []*LogInput) {
		closeRunLogInputs(inputs...)
	}(inputs)

	results, err := runner.Run(inputs...)
	if err != nil {
		return err
	}
	for machineID, process := range results.status {
		m.Set(machineID, process)
	}
	return nil
}

func (p *Participant) newNodeStorageFromSqlite(dbFile string) (NodeStorage, []*LogProcess, error) {
	ns := NodeStorageImpl{}
	ns.Init()

	sqlite := SqliteAdapter{}
	err := sqlite.Init(dbFile)
	if err != nil {
		return nil, nil, err
	}
	defer sqlite.Close()

	processes, err := sqlite.Processes()
	if err != nil {
		return nil, nil, err
	}

	err = ns.Merge(&sqlite)
	if err != nil {
		return nil, nil, err
	}

	return &ns, processes, nil
}

func (p *Participant) runLogTillEnd() error {
	if err := runLog(p.runner, p.network, p.m); err != nil {
		return err
	}
	offset, err := p.w.Offset()
	if err != nil {
		return err
	}
	if p.m.Get(p.me.name).Offset != offset {
		return fmt.Errorf("log process is not yet end")
	}
	return nil
}

func (p *Participant) Init(wd string, machineID string) (err error) {
	wd, err = ToAbs(wd)
	if err != nil {
		return err
	}

	if !IsDir(wd) {
		err := os.MkdirAll(wd, 0777)
		if err != nil {
			return err
		}
	}

	network := NetworkInfo{}
	err = network.Init(wd)
	if err != nil {
		return err
	}

	_, err = initParticipant(wd, machineID, &network)
	if err != nil {
		return err
	}
	me := network.Add(machineID)

	ns, offsets, err := p.newNodeStorageFromSqlite(me.dbFile)
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
	defer func() {
		if err != nil {
			w.Close()
		}
	}()

	p.network = &network
	p.m = &m
	p.ns = ns
	p.w = &w
	p.me = me
	p.runner = &runner
	return nil
}

func (p *Participant) persistToSqlite() error {
	sqlite := SqliteAdapter{}
	err := sqlite.Init(p.me.dbFile)
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
	err = runner.Init(p.me.name, &sqlite)
	if err != nil {
		return err
	}
	return runLog(&runner, p.network, &m)
}

func (p *Participant) Close() {
	if p.w != nil {
		p.w.Close()
		p.w = nil
	}
	logger.Info("persist to sqlite...")
	err := p.persistToSqlite()
	if err != nil {
		logger.Error("persist to sqlite failed[%v]", err)
	}
}

func (p *Participant) Save(key string, value string) error {
	if err := p.runLogTillEnd(); err != nil {
		return err
	}

	leaves, err := p.ns.GetByKey(key)
	if err != nil {
		return err
	}
	leaves = filterVisible(leaves)

	if len(leaves) == 0 {
		_, _, err := p.w.Append(&LogOperation{
			Op:            int32(Op_Modify),
			Key:           key,
			Value:         value,
			PrevGid:       "",
			PrevValue:     "",
			Seq:           0,
			MachineId:     p.me.name,
			PrevMachineId: "",
			Changes:       map[string]int32{p.me.name: 1},
			PrevNum:       0,
		})
		return err
	}

	main := findMain(leaves, p.me.name)
	if main == nil {
		return fmt.Errorf("cannot find main node")
	}

	_, _, err = p.w.Append(&LogOperation{
		Op:            int32(Op_Modify),
		Key:           key,
		Value:         value,
		PrevGid:       main.CurrentLogGid,
		PrevValue:     main.Value,
		Seq:           main.Seq + 1,
		MachineId:     p.me.name,
		PrevMachineId: main.MachineID,
		Changes:       main.AddChange(p.me.name, 1),
		PrevNum:       main.Num,
	})
	return err
}

func (p *Participant) Del(key string) error {
	if err := p.runLogTillEnd(); err != nil {
		return err
	}

	leaves, err := p.ns.GetByKey(key)
	if err != nil {
		return err
	}
	leaves = filterVisible(leaves)
	if len(leaves) == 0 {
		return nil
	}

	main := findMain(leaves, p.me.name)
	if main == nil {
		return fmt.Errorf("cannot find main node")
	}

	_, _, err = p.w.Append(&LogOperation{
		Op:            int32(Op_Del),
		Key:           key,
		PrevGid:       main.CurrentLogGid,
		PrevValue:     main.Value,
		Seq:           main.Seq + 1,
		MachineId:     p.me.name,
		PrevMachineId: main.MachineID,
		Changes:       main.AddChange(p.me.name, 1),
		PrevNum:       main.Num,
	})
	return err
}

func (p *Participant) Has(key string) (bool, error) {
	if err := p.runLogTillEnd(); err != nil {
		return false, err
	}

	leaves, err := p.ns.GetByKey(key)
	if err != nil {
		return false, err
	}
	leaves = filterVisible(leaves)
	if len(leaves) == 0 {
		return false, nil
	}

	main := findMain(leaves, p.me.name)
	if main == nil {
		return false, fmt.Errorf("cannot find main node")
	}
	return true, nil
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

func (p *Participant) Load(key string) (*Value, error) {
	if err := p.runLogTillEnd(); err != nil {
		return nil, err
	}

	leaves, err := p.ns.GetByKey(key)
	if err != nil {
		return nil, err
	}
	leaves = filterVisible(leaves)
	if len(leaves) == 0 {
		return nil, fmt.Errorf("not exist")
	}

	v := Value{}
	err = v.from(leaves, p.me.name)
	if err != nil {
		return nil, err
	}
	return &v, nil
}

func (p *Participant) All() ([]*Value, error) {
	if err := p.runLogTillEnd(); err != nil {
		return nil, err
	}

	leaves, err := p.ns.AllNodes()
	if err != nil {
		return nil, err
	}
	leaves = filterVisible(leaves)
	if len(leaves) == 0 {
		return nil, nil
	}

	m := make(map[string][]*DBRecord)
	for _, l := range leaves {
		m[l.Key] = append(m[l.Key], l)
	}

	results := make([]*Value, 0)
	for _, l := range m {
		v := Value{}
		err := v.from(l, p.me.name)
		if err != nil {
			return nil, err
		}
		results = append(results, &v)
	}
	return results, nil
}

func (p *Participant) makeDiscardOperation(gid string) (*LogOperation, error) {
	record, err := p.ns.GetByGid(gid)
	if err != nil {
		return nil, err
	}

	return &LogOperation{
		Op:            int32(Op_Discard),
		Key:           record.Key,
		PrevGid:       record.CurrentLogGid,
		PrevValue:     record.Value,
		Seq:           record.Seq + 1,
		MachineId:     p.me.name,
		PrevMachineId: record.MachineID,
		Changes:       record.AddChange(p.me.name, 1),
		PrevNum:       record.Num,
	}, nil
}

func (p *Participant) Accept(v *Value, seq int) error {
	if err := p.runLogTillEnd(); err != nil {
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
			op, err := p.makeDiscardOperation(version.gid)
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

	_, _, err := p.w.Append(operations...)
	return err
}

func (p *Participant) AllConflicts() ([]*Value, error) {
	all, err := p.All()
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

func _() {
	var _ Storage = &Participant{}
}
