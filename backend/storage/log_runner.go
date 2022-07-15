package storage

import (
	"container/list"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
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
	end       string
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

type RunLogContext struct {
	input map[string]*LogInput

	status     map[string]string
	changeList *ChangeList
	errs       sync.Map

	coodinator sync.Mutex
	blockNum   int32
	wg         sync.WaitGroup
}

func (c *RunLogContext) Init(i ...*LogInput) {
	c.input = map[string]*LogInput{}
	c.status = make(map[string]string)
	changeList := ChangeList{}
	changeList.Init()
	c.changeList = &changeList
	c.blockNum = 0

	for _, input := range i {
		if input == nil {
			continue
		}
		c.input[input.machineID] = input
	}
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

// TODO not efficient
// TODO leverage to unlock
func (r *LogRunner) runLogWrapper(c *RunLogContext, l *LogInput, logOp *LogOperation) bool {
	c.coodinator.Lock()

	atomic.AddInt32(&c.blockNum, 1)
	for {
		if r.runLogInner(c, logOp) {
			atomic.AddInt32(&c.blockNum, -1)
			c.status[l.machineID] = logOp.Gid
			c.coodinator.Unlock()
			return true
		} else {
			if int(atomic.LoadInt32(&c.blockNum)) >= len(c.input) {
				return false
			}

			c.coodinator.Unlock()
			c.coodinator.Lock()
		}
	}
}

func (r *LogRunner) runLog(c *RunLogContext, l *LogInput) {
	defer c.wg.Done()
	err := l.w.Range(l.start, l.end, func(logOp *LogOperation) bool {
		return r.runLogWrapper(c, l, logOp)
	})
	atomic.AddInt32(&c.blockNum, 1)
	if err != nil {
		logger.Error("run log failed", err)
		c.errs.Store(l.machineID, err)
		return
	}
}

func (r *LogRunner) Run(i ...*LogInput) (*RunLogResult, error) {
	c := RunLogContext{}
	c.Init(i...)

	for _, input := range i {
		c.wg.Add(1)
		go r.runLog(&c, input)
	}
	c.wg.Wait()

	var err *RunLogError = nil
	c.errs.Range(func(key, value any) bool {
		e, ok := value.(error)
		if ok {
			if err == nil {
				err = &RunLogError{}
			}
			err.errs = append(err.errs, e)
		}
		return true
	})

	return &RunLogResult{changeList: c.changeList, status: c.status, err: err}, nil
}
