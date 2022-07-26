package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	math "math"
)

type BinLog struct {
}

// 写失败将破坏文件数据
func (l *BinLog) WriteHeader(f File, header *FileHeader) error {
	_, err := f.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	if header.Size() > HeaderSize-4 {
		return fmt.Errorf("file header too large")
	}

	headBuffer := [HeaderSize]byte{}
	binary.LittleEndian.PutUint32(headBuffer[:4], uint32(header.Size()))
	n, err := header.MarshalToSizedBuffer(headBuffer[4 : 4+header.Size()])
	if err != nil {
		return err
	}
	if n != header.Size() {
		return fmt.Errorf("write size unexpected")
	}

	n, err = f.Write(headBuffer[:])
	if err != nil {
		return err
	}
	if n != len(headBuffer) {
		return fmt.Errorf("write size unexpected")
	}
	return nil
}

// TODO: 更多校验
func (l *BinLog) IsValidFile(f File) (bool, error) {
	fSize, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return false, err
	}
	if fSize < HeaderSize {
		return false, nil
	}
	return true, nil
}

func (l *BinLog) ReadHeader(f File) (*FileHeader, error) {
	valid, err := l.IsValidFile(f)
	if err != nil {
		return nil, err
	}
	if !valid {
		return nil, fmt.Errorf("invalid file")
	}

	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	headerBuffer := [HeaderSize]byte{}
	n, err := f.Read(headerBuffer[:])
	if err != nil {
		return nil, err
	}
	if n != HeaderSize {
		return nil, fmt.Errorf("read size not expected")
	}

	headerDataSize := binary.LittleEndian.Uint32(headerBuffer[:4])
	if headerDataSize == 0 {
		return &FileHeader{}, nil
	}
	if int(headerDataSize) > len(headerBuffer)-4 {
		return nil, fmt.Errorf("invalid file")
	}

	header := FileHeader{}
	err = header.Unmarshal(headerBuffer[4 : 4+headerDataSize])
	if err != nil {
		return nil, err
	}
	return &header, nil
}

// 写失败将破坏文件数据
func (l *BinLog) AppendEntry(f File, pos int64, entry *LogEntry) (int64, error) {
	valid, err := l.IsValidFile(f)
	if err != nil {
		return 0, err
	}
	if !valid {
		return 0, fmt.Errorf("invalid file")
	}

	sz := entry.Size()
	if sz == 0 {
		return 0, nil
	}

	if pos != -1 {
		if pos < HeaderSize {
			return 0, fmt.Errorf("invalid pos")
		}
		_, err := f.Seek(pos, io.SeekStart)
		if err != nil {
			return 0, err
		}
	} else {
		_, err := f.Seek(0, io.SeekEnd)
		if err != nil {
			return 0, err
		}
	}

	writeSize := sz + 8
	if writeSize > math.MaxUint32 {
		return 0, fmt.Errorf("log entry too large")
	}
	entryBuffer := make([]byte, writeSize)
	_, err = entry.MarshalToSizedBuffer(entryBuffer[4 : 4+sz])
	if err != nil {
		return 0, err
	}
	binary.LittleEndian.PutUint32(entryBuffer[0:4], uint32(sz))
	crcSum := crc32.ChecksumIEEE(entryBuffer[4 : len(entryBuffer)-4])
	binary.LittleEndian.PutUint32(entryBuffer[len(entryBuffer)-4:], crcSum)

	writeSz, err := f.Write(entryBuffer)
	if err != nil {
		return 0, err
	}
	if writeSz != len(entryBuffer) {
		return 0, fmt.Errorf("write size not expected")
	}
	return int64(writeSz), nil
}

func (l *BinLog) ReadEntry(f File, pos int64, entry *LogEntry) (int64, error) {
	_, err := f.Seek(pos, io.SeekStart)
	if err != nil {
		return 0, err
	}

	sizeBuffer := [4]byte{}
	readSz, err := f.Read(sizeBuffer[:])
	if err != nil {
		return 0, err
	}
	if readSz != len(sizeBuffer) {
		return 0, fmt.Errorf("read size unexpected")
	}

	size := binary.LittleEndian.Uint32(sizeBuffer[:])
	if size == 0 {
		entry.Reset()
		// size+crc
		return 4 + 4, nil
	}

	entryBuffer := make([]byte, size)
	readSz, err = f.Read(entryBuffer)
	if err != nil {
		return 0, err
	}
	if readSz != len(entryBuffer) {
		return 0, fmt.Errorf("read size unexpected")
	}

	crcSumBuffer := [4]byte{}
	readSz, err = f.Read(crcSumBuffer[:])
	if err != nil {
		return 0, err
	}
	if readSz != len(crcSumBuffer) {
		return 0, fmt.Errorf("read size unexpected")
	}

	crcSumRead := binary.LittleEndian.Uint32(crcSumBuffer[:])
	crcSumCalc := crc32.ChecksumIEEE(entryBuffer)
	if crcSumCalc != crcSumRead {
		return 0, fmt.Errorf("crc checksum mismatch pos[%v]", pos)
	}

	err = entry.Unmarshal(entryBuffer)
	if err != nil {
		return 0, err
	}
	return int64(size + 8), nil
}

func _() {
	var _ LogFormat = &BinLog{}
}
