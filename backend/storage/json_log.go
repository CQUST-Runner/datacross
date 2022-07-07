package storage

type JsonLog struct {
}

func (l *JsonLog) WriteHeader(f File, header *FileHeader) error {
	return nil
}

func (l *JsonLog) IsValidFile(f File) (bool, error) {
	return true, nil
}

func (l *JsonLog) ReadHeader(f File) (*FileHeader, error) {
	return nil, nil
}

func (l *JsonLog) AppendEntry(f File, pos int64, entry *LogEntry) (int64, error) {
	return 0, nil
}

func (l *JsonLog) ReadEntry(f File, pos int64, entry *LogEntry) (int64, error) {
	return 0, nil
}

func (l *JsonLog) Init(filename string) (err error) {
	return nil
}

func (l *JsonLog) Append(entry *LogEntry) error {
	return nil
}

func (l *JsonLog) Replay(s Storage) {

}

func (l *JsonLog) Flush() error {
	return nil
}

func _() {
	var _ LogFile = &JsonLog{}
}
