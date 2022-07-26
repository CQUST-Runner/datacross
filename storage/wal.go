package storage

import (
	"fmt"

	gogoproto "github.com/gogo/protobuf/proto"
)

type Iterator interface {
	Next() bool
}

type WalIterator struct {
	w      *Wal
	pos    int64
	endPos int64 //const
	entry  *LogEntry
	index  int

	stoped bool

	end        string
	includeEnd bool
}

func (i *WalIterator) Init(w *Wal) {
	i.w = w
	i.pos = HeaderSize
	i.endPos = w.header.FileEnd
	i.entry = nil
	i.index = 0
}

func (i *WalIterator) InitWithOffset(w *Wal, offset int64) {
	if offset < HeaderSize {
		offset = HeaderSize
	}
	i.w = w
	i.pos = offset
	i.endPos = w.header.FileEnd
	i.entry = nil
	i.index = 0
}

func (i *WalIterator) Next() (hasNext bool) {
	if i.stoped {
		return false
	}
	defer func() {
		if hasNext {
			if len(i.end) > 0 && i.LogOp().Gid == i.end {
				if i.includeEnd {
					i.stoped = true
				} else {
					hasNext = false
					i.stoped = true
				}
			}
		} else {
			i.stoped = true
		}
	}()
	if i.entry != nil && i.index < len(i.entry.Ops)-1 {
		i.index++
		return true
	}

	var entry *LogEntry = nil
	for i.pos < i.endPos {
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

func (i *WalIterator) Offset() int64 {
	return i.pos
}

type Wal struct {
	f      File
	l      LogFormat
	header *FileHeader
	pos    int64
	broken bool

	readonly bool
}

func (w *Wal) Init(filename string, l LogFormat, readonly bool) (err error) {
	f, err := OpenFile(filename, readonly)
	if err != nil {
		return err
	}
	defer func(f File) {
		if err != nil {
			e := f.Close()
			if e != nil {
				logger.Error("close wal file failed[%v]", e)
			}
		}
	}(f)

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
		header = &FileHeader{Id: id, FileEnd: HeaderSize, EntryNum: 0}
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
	return nil
}

func (w *Wal) Offset() int64 {
	return w.header.FileEnd
}

// Append multiple operations will be appended as a single log entry
// returns the gid of the last operation
// generate and populate .Num, .Gid for each operation
func (w *Wal) Append(logOp ...*LogOperation) (string, int64, error) {
	if w.broken {
		return "", 0, fmt.Errorf("wal is broken")
	}
	if w.readonly {
		return "", 0, fmt.Errorf("append to readonly file")
	}
	if len(logOp) == 0 {
		return "", 0, fmt.Errorf("empty input")
	}

	gids := make([]string, len(logOp))
	for i := range gids {
		gid, err := GenUUID()
		if err != nil {
			return "", 0, err
		}
		gids[i] = gid
	}
	lastNum := w.header.EntryNum + int64(len(logOp))
	for i := range logOp {
		logOp[i].Gid = gids[i]
		logOp[i].Num = w.header.EntryNum + int64(i) + 1
	}
	lastGid := gids[len(gids)-1]

	err := w.AppendRaw(logOp...)
	if err != nil {
		return "", 0, err
	}

	return lastGid, lastNum, nil
}

func (w *Wal) AppendRaw(logOp ...*LogOperation) error {
	if w.broken {
		return fmt.Errorf("wal is broken")
	}
	if w.readonly {
		return fmt.Errorf("append to readonly file")
	}
	if len(logOp) == 0 {
		return fmt.Errorf("empty input")
	}

	writeSz, err := w.l.AppendEntry(w.f, w.pos, &LogEntry{Ops: logOp})
	if err != nil {
		return err
	}
	if writeSz == 0 {
		return fmt.Errorf("write size is 0, suspicious")
	}

	newPos := w.pos + writeSz
	newHeader := gogoproto.Clone(w.header).(*FileHeader)
	newHeader.FileEnd = newPos
	newHeader.LastEntryId = logOp[len(logOp)-1].Gid
	newHeader.EntryNum += int64(len(logOp))
	err = w.l.WriteHeader(w.f, newHeader)
	if err != nil {
		w.broken = true
		return err
	}

	w.pos = newPos
	w.header = newHeader
	return nil
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

func (w *Wal) Iterator() *WalIterator {
	i := WalIterator{}
	i.Init(w)
	return &i
}

func (w *Wal) IteratorOffset(offset int64) *WalIterator {
	i := WalIterator{}
	i.InitWithOffset(w, offset)
	return &i
}

func (w *Wal) IteratorFrom(start string, inclusive bool) (*WalIterator, error) {
	i := WalIterator{}
	i.Init(w)
	if len(start) == 0 {
		return &i, nil
	}

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
