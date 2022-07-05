package storage

import (
	"io"
)

const HeaderSize = 0x100

type LogFile struct {
	f File
}

func writeHeader(f File, header *FileHeader) error {

	_, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	return nil
}

func readHeader(f File) error {

	fSize, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	id, err := GenUUID()
	if err != nil {
		return err
	}
	if fSize < HeaderSize {
		newHeader := FileHeader{Id: id}
		_ = newHeader

	}

	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	return nil
}

func (f *LogFile) Init(filename string) error {
	ff, err := OpenFile(filename)
	if err != nil {
		return err
	}
	f.f = ff
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
