package storage

const HeaderSize = 0x100

type LogFile interface {
	WriteHeader(f File, header *FileHeader) error
	IsValidFile(f File) (bool, error)
	ReadHeader(f File) (*FileHeader, error)
	AppendEntry(f File, pos int64, entry *LogEntry) (int64, error)
	ReadEntry(f File, pos int64, entry *LogEntry) (int64, error)
}
