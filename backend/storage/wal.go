package storage

import (
	"fmt"

	gogoproto "github.com/gogo/protobuf/proto"
)

type Wal struct {
	f      File
	l      LogFile
	header *FileHeader
	pos    int64
	broken bool
}

func (w *Wal) Init(filename string, l LogFile) error {
	f, err := OpenFile(filename)
	if err != nil {
		return err
	}

	valid, err := l.IsValidFile(f)
	if err != nil {
		return err
	}
	var header *FileHeader
	if !valid {
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

func (w *Wal) Append(op int32, key string, value string) error {
	if w.broken {
		return fmt.Errorf("wal is broken")
	}

	gid, err := GenUUID()
	if err != nil {
		return err
	}
	entry := LogEntry{Op: op, Key: key, Value: value, Gid: gid}
	writeSz, err := w.l.AppendEntry(w.f, w.pos, &entry)
	if err != nil {
		return err
	}
	if writeSz == 0 {
		return nil
	}

	newPos := w.pos + writeSz
	newHeader := gogoproto.Clone(w.header).(*FileHeader)
	newHeader.FileEnd = newPos
	err = w.l.WriteHeader(w.f, newHeader)
	// This should not be happen
	if err != nil {
		w.broken = true
		return err
	}

	w.pos = newPos
	w.header = newHeader
	return nil
}

func execLogEntry(entry *LogEntry, s Storage) error {
	switch entry.Op {
	case int32(Op_Add):
		return s.Save(entry.Key, entry.Value)
	case int32(Op_Del):
		return s.Del(entry.Key)
	case int32(Op_Modify):
		return s.Save(entry.Key, entry.Value)
	case int32(Op_Accept):
		return fmt.Errorf("unsupported op[%v]", entry.Op)
	case int32(Op_None):
		return fmt.Errorf("unrecognized op[%v]", entry.Op)
	default:
		return fmt.Errorf("unrecognized op[%v]", entry.Op)
	}
}

func foreach(w *Wal, do func(entry *LogEntry) bool) error {
	var pos int64 = HeaderSize
	for pos < w.header.FileEnd {
		entry := LogEntry{}
		readSz, err := w.l.ReadEntry(w.f, pos, &entry)
		if err != nil {
			return err
		}
		if readSz <= 0 {
			return fmt.Errorf("read size unexpected")
		}

		if !do(&entry) {
			break
		}
		pos += readSz
	}
	return nil
}

func replay(w *Wal, s Storage, start string, end string) error {
	inRange := len(start) == 0
	var e error
	err := foreach(w, func(entry *LogEntry) bool {
		if len(start) > 0 && entry.Gid == start {
			inRange = true
		}
		if len(end) > 0 && entry.Gid == end && inRange {
			inRange = false
		}
		if inRange {
			err := execLogEntry(entry, s)
			if err != nil {
				e = err
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

func (w *Wal) Replay(s Storage, start string) error {
	if w.broken {
		return fmt.Errorf("wal is broken")
	}
	return replay(w, s, start, "")
}

func (w *Wal) Flush() error {
	if w.broken {
		return fmt.Errorf("wal is broken")
	}

	return w.f.Flush()
}
