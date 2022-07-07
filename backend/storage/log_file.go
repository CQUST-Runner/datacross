package storage

type LogFile interface {
	WriteHeader(f File, header *FileHeader) error
	IsValidFile(f File) (bool, error)
	ReadHeader(f File) (*FileHeader, error)
	AppendEntry(f File, pos uint, entry *LogEntry) error
	ReadEntry(f File, pos int64, entry *LogEntry) (uint32, error)
	Flush() error
}
