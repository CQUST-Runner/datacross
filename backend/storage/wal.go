package storage

const HeaderSize = 0x100

type Wal struct {
	f File
	l LogFile
}

func (w *Wal) Init(filename string) error {
	ff, err := OpenFile(filename)
	if err != nil {
		return err
	}
	w.f = ff
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
