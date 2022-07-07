package storage

// TODO: 添加log库

type Wal struct {
	f      File
	l      LogFile
	header *FileHeader
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
	return nil
}

func (w *Wal) Append(entry *LogEntry) error {
	return nil
}

func (w *Wal) Replay(s Storage) {
}

func (w *Wal) Flush() error {
	return nil
}
