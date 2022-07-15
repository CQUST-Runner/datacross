package storage

import (
	"fmt"

	gogoproto "github.com/gogo/protobuf/proto"
)

type Iterator interface {
	Next() bool
}

type WalIterator struct {
	w     *Wal
	pos   int64
	entry *LogEntry
	index int

	stoped bool

	preStop    bool
	end        string
	includeEnd bool
}

func (i *WalIterator) Init(w *Wal) {
	i.w = w
	i.pos = HeaderSize
	i.entry = nil
	i.index = 0
}

func (i *WalIterator) Next() (hasNext bool) {
	if i.stoped {
		return false
	}
	if i.preStop {
		return false
	}
	defer func() {
		if hasNext {
			if len(i.end) > 0 && i.LogOp().Gid == i.end {
				if i.includeEnd {
					i.preStop = true
				} else {
					hasNext = false
				}
			}
		}
		if !hasNext {
			i.stoped = true
		}
	}()
	if i.entry != nil && i.index < len(i.entry.Ops)-1 {
		i.index++
		return true
	}

	var entry *LogEntry = nil
	for i.pos < i.w.header.FileEnd {
		tmp := LogEntry{}
		readSz, err := i.w.l.ReadEntry(i.w.f, i.pos, &tmp)
		if err != nil {
			return false
		}
		if readSz <= 0 {
			return false
		}
		i.pos += readSz

		if len(tmp.Ops) > 0 {
			entry = &tmp
			break
		}
	}
	if entry == nil {
		return false
	}

	i.entry = entry
	i.index = 0
	return true
}

func (i *WalIterator) LogOp() *LogOperation {
	if i.entry == nil || i.index >= len(i.entry.Ops) {
		panic("error state")
	}
	return i.entry.Ops[i.index]
}

type Wal struct {
	f      File
	l      LogFormat
	header *FileHeader
	pos    int64
	broken bool

	readonly bool
}

func (w *Wal) Init(filename string, l LogFormat, readonly bool) error {
	f, err := OpenFile(filename, readonly)
	if err != nil {
		return err
	}

	valid, err := l.IsValidFile(f)
	if err != nil {
		return err
	}
	var header *FileHeader
	if !valid {
		if readonly {
			return fmt.Errorf("invalid wal file")
		}
		id, err := GenUUID()
		if err != nil {
			return err
		}
		header = &FileHeader{Id: id, FileEnd: HeaderSize}
		err = l.WriteHeader(f, header)
		if err != nil {
			return err
		}
	} else {
		header, err = l.ReadHeader(f)
		if err != nil {
			return err
		}
	}

	w.header = header
	w.f = f
	w.l = l
	w.pos = header.FileEnd
	w.broken = false
	w.readonly = readonly
	return nil
}

func (w *Wal) Close() error {
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	if err != nil {
		logger.Error("close wal file[%v] failed[%v]", w.f.Path(), err)
		return err
	}
	w.f = nil
	w.header = nil
	w.broken = false
	w.l = nil
	w.pos = 0
	return nil
}

// Append multiple operations will be appended as a single log entry
// returns the gid of the last operation
func (w *Wal) Append(logOp ...*LogOperation) (string, error) {
	if w.broken {
		return "", fmt.Errorf("wal is broken")
	}
	if w.readonly {
		return "", fmt.Errorf("append to readonly file")
	}
	if len(logOp) == 0 {
		return "", fmt.Errorf("empty input")
	}

	gids := make([]string, len(logOp))
	for i := range gids {

		gid, err := GenUUID()
		if err != nil {
			return "", err
		}
		gids[i] = gid
	}
	for i := range logOp {
		logOp[i].Gid = gids[i]
	}
	lastGid := gids[len(gids)-1]
	writeSz, err := w.l.AppendEntry(w.f, w.pos, &LogEntry{Ops: logOp})
	if err != nil {
		return "", err
	}
	if writeSz == 0 {
		return "", fmt.Errorf("write size is 0, suspicious")
	}

	newPos := w.pos + writeSz
	newHeader := gogoproto.Clone(w.header).(*FileHeader)
	newHeader.FileEnd = newPos
	newHeader.LastEntryId = lastGid
	err = w.l.WriteHeader(w.f, newHeader)
	// This should not be happen
	if err != nil {
		w.broken = true
		return "", err
	}

	w.pos = newPos
	w.header = newHeader
	return lastGid, nil
}

func execLogEntry(logOp *LogOperation, s Storage) error {
	switch logOp.Op {
	case int32(Op_Del):
		return s.WithCommitID(logOp.Gid).Del(logOp.Key)
	case int32(Op_Modify):
		return s.WithCommitID(logOp.Gid).Save(logOp.Key, logOp.Value)
	case int32(Op_Discard):
		return fmt.Errorf("unsupported op[%v]", logOp.Op)
	case int32(Op_None):
		return fmt.Errorf("unrecognized op[%v]", logOp.Op)
	default:
		return fmt.Errorf("unrecognized op[%v]", logOp.Op)
	}
}

func foreach(w *Wal, perEntry bool, do func(pos int64, entry *LogEntry, index int) bool) error {
	var pos int64 = HeaderSize
	for pos < w.header.FileEnd {
		entry := new(LogEntry)
		readSz, err := w.l.ReadEntry(w.f, pos, entry)
		if err != nil {
			return err
		}
		if readSz <= 0 {
			return fmt.Errorf("read size unexpected")
		}

		term := false
		if !perEntry {
			for index, logOp := range entry.Ops {
				if logOp == nil {
					continue
				}
				if !do(pos, entry, index) {
					term = true
					break
				}
			}
		} else {
			term = !do(pos, entry, 0)
		}
		if term {
			break
		}
		pos += readSz
	}
	return nil
}

// Replay start not included
func (w *Wal) Replay(s Storage, start string) error {
	if w.broken {
		return fmt.Errorf("wal is broken")
	}
	return foreachIn(w, func(_ int64, entry *LogEntry, index int) bool {
		err := execLogEntry(entry.Ops[index], s)
		if err != nil {
			return false
		}
		return true
	}, start, "")
}

func (w *Wal) Flush() error {
	if w.broken {
		return fmt.Errorf("wal is broken")
	}
	if w.readonly {
		return nil
	}

	return w.f.Flush()
}

func (w *Wal) Foreach(do func(logOp *LogOperation) bool) error {
	return foreach(w, false, func(_ int64, entry *LogEntry, index int) bool {
		return do(entry.Ops[index])
	})
}

func foreachInInternal(w *Wal, do func(pos int64, entry *LogEntry, index int) bool, start string, end string, includeStart bool, includeEnd bool) error {
	preInRange := false
	preOutRange := false
	inRange := len(start) == 0
	var e error
	err := foreach(w, false, func(pos int64, entry *LogEntry, index int) bool {
		logOp := entry.Ops[index]
		if preInRange {
			inRange = true
			preInRange = false
		}
		if preOutRange {
			inRange = false
			return false
		}

		if len(start) > 0 && logOp.Gid == start {
			if includeStart {
				inRange = true
			} else {
				preInRange = true
			}
		}
		if len(end) > 0 && logOp.Gid == end && inRange {
			if includeEnd {
				preOutRange = true
			} else {
				inRange = false
				return false
			}
		}

		if inRange {
			if !do(pos, entry, index) {
				return false
			}
		}
		return true
	})

	if err != nil {
		return err
	}
	return e
}

// with includeStart, includeEnd = false, false
func foreachIn(w *Wal, do func(pos int64, entry *LogEntry, index int) bool, start string, end string) error {
	return foreachInInternal(w, do, start, end, false, false)
}

func (w *Wal) Range(start string, end string, do func(logOp *LogOperation) bool) error {
	return foreachIn(w, func(_ int64, entry *LogEntry, index int) bool {
		return do(entry.Ops[index])
	}, start, end)
}

func (w *Wal) Iterator() *WalIterator {
	i := WalIterator{}
	i.Init(w)
	return &i
}

func (w *Wal) IteratorFrom(start string, inclusive bool) (*WalIterator, error) {
	i := WalIterator{}
	i.Init(w)
	found := false
	for i.Next() {
		if i.LogOp().Gid == start {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("start position not found")
	}
	if inclusive {
		i.index -= 1
	}
	return &i, nil
}

func (w *Wal) RangeIterator(start string, end string, includeStart bool, includeEnd bool) (*WalIterator, error) {
	i, err := w.IteratorFrom(start, includeStart)
	if err != nil {
		return nil, err
	}
	i.end = end
	i.includeEnd = includeEnd
	return i, nil
}
