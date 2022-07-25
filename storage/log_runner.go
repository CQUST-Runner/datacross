package storage

import (
	"fmt"
	"strings"
)

type LogInput struct {
	machineID string
	w         *Wal
	process   *LogProcess
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
	status map[string]*LogProcess
	err    *RunLogError
}

func (r *RunLogResult) Init(s map[string]*LogProcess, e *RunLogError) {
	r.status = s
	r.err = e
}

func (r *RunLogResult) Position(machineID string) string {
	if pos, ok := r.status[machineID]; ok {
		return pos.Gid
	}
	return ""
}

func (r *RunLogResult) Error() error {
	return r.err
}

type RunLogWorker struct {
	input            *LogInput
	process          *LogProcess
	err              error
	pendingOp        *LogOperation
	pendingOpProcess *LogProcess
	it               *WalIterator
}

type RunLogContext struct {
	workers map[string]*RunLogWorker
}

func (c *RunLogContext) Init(i ...*LogInput) error {
	c.workers = make(map[string]*RunLogWorker)

	for _, input := range i {
		if input == nil {
			continue
		}
		it := input.w.IteratorOffset(input.process.Offset)
		c.workers[input.machineID] = &RunLogWorker{
			input:   input,
			process: input.process,
			it:      it,
		}
	}
	return nil
}

func (c *RunLogContext) Progress(machineID string) int64 {
	if w, ok := c.workers[machineID]; ok {
		return w.process.Num
	}
	return 0
}

type LogRunner struct {
	machineID string
	s         NodeStorage
}

func (r *LogRunner) Init(machineID string, s NodeStorage) error {
	r.machineID = machineID
	r.s = s
	return nil
}

func (r *LogRunner) runLogInner(c *RunLogContext, process *LogProcess, logOp *LogOperation) bool {
	if logOp.PrevNum == 0 {
		record := DBRecord{
			Key:                logOp.Key,
			Value:              logOp.Value,
			MachineID:          logOp.MachineId,
			Offset:             process.Offset,
			Seq:                logOp.Seq,
			CurrentLogGid:      logOp.Gid,
			IsDeleted:          logOp.Op == int32(Op_Del),
			IsDiscarded:        logOp.Op == int32(Op_Discard),
			MachineChangeCount: map[string]int32{logOp.MachineId: 1},
			Num:                logOp.Num,
			PrevNum:            logOp.PrevNum,
		}
		err := r.s.Add(&record)
		if err != nil {
			logger.Error("add leaf[%v] [%v] failed[%v]", record.Key, record.CurrentLogGid, err)
			return false
		}
		return true
	}

	if logOp.PrevNum > c.Progress(logOp.PrevMachineId) {
		return false
	}

	parent, err := r.s.GetByGid(logOp.PrevGid)
	if err != nil {
		return false
	}
	if parent != nil {
		record := DBRecord{
			Key:                logOp.Key,
			Value:              logOp.Value,
			MachineID:          logOp.MachineId,
			Offset:             process.Offset,
			PrevMachineID:      parent.MachineID,
			Seq:                logOp.Seq,
			CurrentLogGid:      logOp.Gid,
			PrevLogGid:         parent.CurrentLogGid,
			IsDeleted:          logOp.Op == int32(Op_Del),
			IsDiscarded:        logOp.Op == int32(Op_Discard),
			MachineChangeCount: logOp.Changes,
			Num:                logOp.Num,
			PrevNum:            logOp.PrevNum,
		}
		err := r.s.Replace(parent.CurrentLogGid, &record)
		if err != nil {
			logger.Error("update leaf of [%v] [%v]->[%v] failed", parent.Key, parent.CurrentLogGid, record.CurrentLogGid)
			return false
		}
		return true
	}

	record := DBRecord{
		Key:                logOp.Key,
		Value:              logOp.Value,
		MachineID:          logOp.MachineId,
		Offset:             process.Offset,
		PrevMachineID:      logOp.PrevMachineId,
		Seq:                logOp.Seq,
		CurrentLogGid:      logOp.Gid,
		PrevLogGid:         logOp.PrevGid,
		IsDeleted:          logOp.Op == int32(Op_Del),
		IsDiscarded:        logOp.Op == int32(Op_Discard),
		MachineChangeCount: logOp.Changes,
		Num:                logOp.Num,
		PrevNum:            logOp.PrevNum,
	}
	err = r.s.Add(&record)
	if err != nil {
		logger.Error("add leaf of key[%v] [%v] failed", record.Key, record.CurrentLogGid)
		return false
	}
	return true
}

func (r *LogRunner) tryAdvance(c *RunLogContext, worker *RunLogWorker) bool {
	count := 0

	if worker.pendingOp != nil {
		if !r.runLogInner(c, worker.pendingOpProcess, worker.pendingOp) {
			return false
		}
		worker.process = worker.pendingOpProcess
		worker.pendingOp = nil
		worker.pendingOpProcess = nil
		count++
	}

	for worker.it.Next() {
		logOp := worker.it.LogOp()
		currentProcess := LogProcess{
			Num:    logOp.Num,
			Offset: worker.it.Offset(),
			Gid:    logOp.Gid,
		}
		if !r.runLogInner(c, &currentProcess, logOp) {
			worker.pendingOp = logOp
			worker.pendingOpProcess = &currentProcess
			return count > 0
		}
		worker.process = &currentProcess
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

	result := RunLogResult{status: make(map[string]*LogProcess)}
	for _, worker := range c.workers {
		if worker.err != nil {
			if result.err == nil {
				result.err = &RunLogError{}
			}
			result.err.errs = append(result.err.errs, worker.err)
		}
		result.status[worker.input.machineID] = worker.process
	}
	return &result, nil
}
