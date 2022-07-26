package storage

import (
	"fmt"
	"strings"
)

type LogInput struct {
	machineID string
	w         *Wal
	progress  *LogProgress
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
	status map[string]*LogProgress
	err    *RunLogError
}

func (r *RunLogResult) Init(s map[string]*LogProgress, e *RunLogError) {
	r.status = s
	r.err = e
}

func (r *RunLogResult) Error() error {
	return r.err
}

func (r *RunLogResult) Process(machineID string) *LogProgress {
	if progress, ok := r.status[machineID]; ok {
		return progress
	}
	return newLogProgress(machineID)
}

type RunLogWorker struct {
	input            *LogInput
	progress         *LogProgress
	err              error
	pendingOp        *LogOperation
	pendingOpProcess *LogProgress
	it               *WalIterator
}

type RunLogContext struct {
	workers map[string]*RunLogWorker
}

func (c *RunLogContext) Init(i ...*LogInput) {
	c.workers = make(map[string]*RunLogWorker)

	for _, input := range i {
		if input == nil {
			continue
		}
		it := input.w.IteratorOffset(input.progress.Offset)
		c.workers[input.machineID] = &RunLogWorker{
			input:    input,
			progress: input.progress,
			it:       it,
		}
	}
}

func (c *RunLogContext) Progress(machineID string) *LogProgress {
	if w, ok := c.workers[machineID]; ok {
		return w.progress
	}
	return newLogProgress(machineID)
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

func (r *LogRunner) runLogInner(c *RunLogContext, progress *LogProgress, logOp *LogOperation) bool {
	if logOp.PrevNum == 0 {
		record := DBRecord{
			Key:                logOp.Key,
			Value:              logOp.Value,
			MachineID:          logOp.MachineId,
			PrevMachineID:      "",
			Offset:             progress.Offset,
			Seq:                logOp.Seq,
			CurrentLogGid:      logOp.Gid,
			PrevLogGid:         "",
			IsDeleted:          logOp.Op == int32(Op_Del),
			IsDiscarded:        logOp.Op == int32(Op_Discard),
			MachineChangeCount: logOp.Changes,
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

	if logOp.PrevNum > c.Progress(logOp.PrevMachineId).Num {
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
			Offset:             progress.Offset,
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
		Offset:             progress.Offset,
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
		worker.progress = worker.pendingOpProcess
		worker.pendingOp = nil
		worker.pendingOpProcess = nil
		count++
	}

	for worker.it.Next() {
		logOp := worker.it.LogOp()
		currentProcess := LogProgress{
			Num:    logOp.Num,
			Offset: worker.it.Offset(),
			Gid:    logOp.Gid,
		}
		if !r.runLogInner(c, &currentProcess, logOp) {
			worker.pendingOp = logOp
			worker.pendingOpProcess = &currentProcess
			return count > 0
		}
		worker.progress = &currentProcess
		count++
	}
	return count > 0
}

func (r *LogRunner) Run(i ...*LogInput) (*RunLogResult, error) {
	if len(i) == 0 {
		return nil, fmt.Errorf("empty input")
	}
	c := RunLogContext{}
	c.Init(i...)

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

	result := RunLogResult{status: make(map[string]*LogProgress)}
	for _, worker := range c.workers {
		if worker.err != nil {
			if result.err == nil {
				result.err = &RunLogError{}
			}
			result.err.errs = append(result.err.errs, worker.err)
		}
		result.status[worker.input.machineID] = worker.progress
	}
	return &result, nil
}
