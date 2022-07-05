package storage

const (
	Add    = 0x1
	Modify = 0x2
	Del    = 0x3
	Accept = 0x4
)

// LogEntry
type LogEntry struct {
	Op    uint8
	Key   string
	Value string
}

type LogFile struct {
	fd uintptr
}

func (f *LogFile) Init(filename string) error {
	fd, err := OpenFile(filename)
	if err != nil {
		return err
	}
	f.fd = fd
	return nil
}

func (f *LogFile) Append(entry *LogEntry) error {
	return nil
}

func (f *LogFile) Replay(s Storage) {

}

func (f *LogFile) Flush() error {
	return nil
}
