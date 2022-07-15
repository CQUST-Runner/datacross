package storage

import (
	"container/list"
	"fmt"
	"strings"
)

type LeafStorage interface {
	AddLeaf(record *DBRecord) error
	GetLeavesByKey(key string) ([]*DBRecord, error)
	// UpdateLeaf the key of old and new should be same
	UpdateLeaf(old *DBRecord, new *DBRecord) error
}

type MemLeafStorage struct {
	leaves   *list.List
	keyIndex map[string][]*list.Element
	gidIndex map[string]*list.Element
}

func (m *MemLeafStorage) Init(initial []*DBRecord) {
	m.leaves = list.New()
	m.keyIndex = make(map[string][]*list.Element)
	m.gidIndex = make(map[string]*list.Element)

	for _, record := range initial {
		if record == nil {
			continue
		}
		_ = m.AddLeaf(record)
	}
}

func (m *MemLeafStorage) AddLeaf(record *DBRecord) error {
	e := m.leaves.PushBack(record)
	m.keyIndex[record.Key] = append(m.keyIndex[record.Key], e)
	m.gidIndex[record.CurrentLogGid] = e
	return nil
}

func (m *MemLeafStorage) GetLeavesByKey(key string) ([]*DBRecord, error) {
	earr, ok := m.keyIndex[key]
	if !ok {
		return nil, nil
	}
	result := []*DBRecord{}
	for _, e := range earr {
		if e == nil {
			continue
		}
		record, ok := e.Value.(*DBRecord)
		if !ok {
			continue
		}
		if record.Visible() {
			result = append(result, record)
		}
	}
	return result, nil
}

func (m *MemLeafStorage) UpdateLeaf(old *DBRecord, new *DBRecord) error {
	e, ok := m.gidIndex[old.CurrentLogGid]
	if !ok {
		return fmt.Errorf("old record not exist")
	}
	e.Value = new
	delete(m.gidIndex, old.CurrentLogGid)
	m.gidIndex[new.CurrentLogGid] = e
	return nil
}

func (s *MemLeafStorage) WithCommitID(string) Storage {
	return s
}

func (s *MemLeafStorage) WithMachineID(string) Storage {
	return s
}

func (s *MemLeafStorage) Save(key string, value string) error {
	return nil
}

func (s *MemLeafStorage) Del(key string) error {
	return nil
}

func (s *MemLeafStorage) Has(key string) (bool, error) {
	return false, nil
}

func (s *MemLeafStorage) Load(key string) (string, error) {
	return "", fmt.Errorf("not exist")
}

func (s *MemLeafStorage) All() ([][2]string, error) {
	return nil, nil
}

func (s *MemLeafStorage) Merge(Storage) error {
	return fmt.Errorf("unsupported")
}

func (s *MemLeafStorage) Discard(key string, gids []string) error {
	return fmt.Errorf("unsupported")
}

func init() {
	var _ LeafStorage = &MemLeafStorage{}
	var _ Storage = &MemLeafStorage{}
}

type LogInput struct {
	machineID string
	w         *Wal
	start     string
}

type ChangeList struct {
	newRecords []*DBRecord
	changeList map[string]*DBRecord
	lasts      map[string]struct{}
}

func (c *ChangeList) Init() {
	c.changeList = map[string]*DBRecord{}
	c.lasts = make(map[string]struct{})
}

func (c *ChangeList) UpdateChangeList(old *DBRecord, new *DBRecord) error {
	if old == nil {
		c.newRecords = append(c.newRecords, new)
		return nil
	}

	if new.Key != old.Key {
		return fmt.Errorf("old key and new key must be the same")
	}
	if new.PrevLogGid != old.CurrentLogGid {
		return fmt.Errorf("new record must be a follower of the old record")
	}

	if _, ok := c.changeList[old.CurrentLogGid]; !ok {
		c.changeList[old.CurrentLogGid] = old
		c.changeList[new.CurrentLogGid] = new
		c.lasts[new.CurrentLogGid] = struct{}{}
	} else {
		c.changeList[new.CurrentLogGid] = new
		delete(c.lasts, old.CurrentLogGid)
		c.lasts[new.CurrentLogGid] = struct{}{}
	}
	return nil
}

func (c *ChangeList) NewRecords() []*DBRecord {
	return c.newRecords
}

func (c *ChangeList) ChangedRecords() [][]*DBRecord {
	result := [][]*DBRecord{}
	for gid := range c.lasts {
		history := []*DBRecord{}
		for {
			record, ok := c.changeList[gid]
			if !ok {
				break
			}
			// must be slow!!!
			history = append([]*DBRecord{record}, history...)
			gid = record.PrevLogGid
		}
		result = append(result, history)
	}
	return result
}

type RunLogError struct {
	errs []error
}

func (e *RunLogError) Error() string {
	sb := strings.Builder{}
	for _, e1 := range e.errs {
		if e1 == nil {
			continue
		}
		sb.WriteString(fmt.Sprintln(e1.Error()))
	}
	return sb.String()
}

type RunLogResult struct {
	changeList *ChangeList
	status     map[string]string
	err        *RunLogError
}

func (r *RunLogResult) Init(l *ChangeList, s map[string]string, e *RunLogError) {
	r.changeList = l
	r.status = s
	r.err = e
}

func (r *RunLogResult) ChangeList() *ChangeList {
	return r.changeList
}

func (r *RunLogResult) Position(machineID string) string {
	if pos, ok := r.status[machineID]; ok {
		return pos
	}
	return ""
}

func (r *RunLogResult) Error() error {
	return r.err
}

type RunLogWorker struct {
	input     *LogInput
	position  string
	err       error
	pendingOp *LogOperation
	it        *WalIterator
}

type RunLogContext struct {
	workers    map[string]*RunLogWorker
	changeList *ChangeList
}

func (c *RunLogContext) Init(i ...*LogInput) error {
	c.workers = make(map[string]*RunLogWorker)
	changeList := ChangeList{}
	changeList.Init()
	c.changeList = &changeList

	for _, input := range i {
		if input == nil {
			continue
		}
		it, err := input.w.IteratorFrom(input.start, false)
		if err != nil {
			return err
		}
		c.workers[input.machineID] = &RunLogWorker{
			input:    input,
			position: input.start,
			it:       it,
		}
	}
	return nil
}

type LogRunner struct {
	machineID string
	s         LeafStorage
}

func (r *LogRunner) Init(machineID string, s LeafStorage) error {
	r.machineID = machineID
	r.s = s
	return nil
}

func (r *LogRunner) runLogInner(c *RunLogContext, logOp *LogOperation) bool {
	if logOp.Seq == 0 {
		record := DBRecord{
			Key:           logOp.Key,
			Value:         logOp.Value,
			MachineID:     logOp.MachineId,
			Seq:           logOp.Seq,
			CurrentLogGid: logOp.Gid,
			IsDeleted:     logOp.Op == int32(Op_Del),
			IsDiscarded:   logOp.Op == int32(Op_Discard),
			IsMain:        true,
		}
		// TODO handle error
		c.changeList.UpdateChangeList(nil, &record)
		err := r.s.AddLeaf(&record)
		if err != nil {
			logger.Error("add leaf[%v] [%v] failed", record.Key, record.CurrentLogGid)
			return false
		}
		return true
	}

	leavesOfKey, err := r.s.GetLeavesByKey(logOp.Key)
	if err != nil {
		logger.Error("get leaves[%v] failed[%v]", logOp.Key, err)
		return false
	}
	var parent *DBRecord = nil
	isSmallOrEqual := true
	for _, leaf := range leavesOfKey {
		if logOp.Seq == leaf.Seq+1 && logOp.PrevGid == leaf.CurrentLogGid {
			parent = leaf
			break
		}
		if logOp.Seq > leaf.Seq {
			isSmallOrEqual = false
		}
	}
	if parent != nil {
		record := DBRecord{
			Key:           logOp.Key,
			Value:         logOp.Value,
			MachineID:     logOp.MachineId,
			PrevMachineID: parent.MachineID,
			Seq:           logOp.Seq,
			CurrentLogGid: logOp.Gid,
			PrevLogGid:    parent.CurrentLogGid,
			IsDeleted:     logOp.Op == int32(Op_Del),
			IsDiscarded:   logOp.Op == int32(Op_Discard),
			IsMain:        true,
		}
		// TODO handle error
		c.changeList.UpdateChangeList(parent, &record)
		err := r.s.UpdateLeaf(parent, &record)
		if err != nil {
			logger.Error("update leaf of [%v] [%v]->[%v] failed", parent.Key, parent.CurrentLogGid, record.CurrentLogGid)
			return false
		}
		return true
	}

	if isSmallOrEqual && logOp.PrevMachineId == r.machineID {
		record := DBRecord{
			Key:           logOp.Key,
			Value:         logOp.Value,
			MachineID:     logOp.MachineId,
			PrevMachineID: logOp.PrevMachineId,
			Seq:           logOp.Seq,
			CurrentLogGid: logOp.Gid,
			PrevLogGid:    logOp.PrevGid,
			IsDeleted:     logOp.Op == int32(Op_Del),
			IsDiscarded:   logOp.Op == int32(Op_Discard),
			IsMain:        true,
		}
		// TODO handle error
		c.changeList.UpdateChangeList(nil, &record)
		err := r.s.AddLeaf(&record)
		if err != nil {
			logger.Error("add leaf of key[%v] [%v] failed", record.Key, record.CurrentLogGid)
			return false
		}
		return true
	}

	return false
}

func (r *LogRunner) tryAdvance(c *RunLogContext, worker *RunLogWorker) bool {
	count := 0

	if worker.pendingOp != nil {
		if !r.runLogInner(c, worker.pendingOp) {
			return false
		}
		worker.position = worker.pendingOp.Gid
		worker.pendingOp = nil
		count++
	}

	for worker.it.Next() {
		logOp := worker.it.LogOp()
		if !r.runLogInner(c, logOp) {
			worker.pendingOp = logOp
			return count > 0
		}
		worker.position = logOp.Gid
		count++
	}
	return count > 0
}

func (r *LogRunner) Run(i ...*LogInput) (*RunLogResult, error) {
	if len(i) == 0 {
		return nil, fmt.Errorf("empty input")
	}
	c := RunLogContext{}
	err := c.Init(i...)
	if err != nil {
		return nil, err
	}

	blockNum := 0
	for {
		for _, worker := range c.workers {
			if r.tryAdvance(&c, worker) {
				blockNum = 0
			} else {
				blockNum++
			}
			if blockNum >= len(c.workers) {
				break
			}
		}
		if blockNum >= len(c.workers) {
			break
		}
	}

	result := RunLogResult{status: make(map[string]string), changeList: c.changeList}
	for _, worker := range c.workers {
		if worker.err != nil {
			if result.err == nil {
				result.err = &RunLogError{}
			}
			result.err.errs = append(result.err.errs, worker.err)
		}
		result.status[worker.input.machineID] = worker.position
	}
	return &result, nil
}
