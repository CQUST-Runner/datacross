package storage

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// TODO set json flag to output single line json

type JsonLog struct {
}

// 写失败将破坏文件数据
func (l *JsonLog) WriteHeader(f File, header *FileHeader) error {
	const available = HeaderSize - 1
	_, err := f.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	jDoc, err := json.Marshal(header)
	if err != nil {
		return err
	}
	if len(jDoc) > available {
		return fmt.Errorf("header too large")
	}

	padding := available - len(jDoc)
	if padding > 0 {
		spaces := bytes.Repeat([]byte{' '}, padding)
		jDoc = append(jDoc, spaces...)
	}
	// now its length equal to HeaderSize
	jDoc = append(jDoc, '\n')

	n, err := f.Write(jDoc)
	if err != nil {
		return err
	}
	if n != len(jDoc) {
		return fmt.Errorf("write size unexpected")
	}
	return nil
}

// TODO: 更多校验
func (l *JsonLog) IsValidFile(f File) (bool, error) {
	fSize, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return false, err
	}
	if fSize < HeaderSize {
		return false, nil
	}
	return true, nil
}

func (l *JsonLog) ReadHeader(f File) (*FileHeader, error) {
	const available = HeaderSize - 1
	_, err := f.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	buffer := [available]byte{}
	n, err := f.Read(buffer[:])
	if err != nil {
		return nil, err
	}
	if n != len(buffer) {
		return nil, fmt.Errorf("read size unexpected")
	}

	header := FileHeader{}
	err = json.Unmarshal(buffer[:], &header)
	if err != nil {
		return nil, err
	}
	return &header, nil
}

// 写失败将破坏文件数据
func (l *JsonLog) AppendEntry(f File, pos int64, entry *LogEntry) (int64, error) {
	jDoc, err := json.Marshal(entry)
	if err != nil {
		return 0, err
	}
	jDoc = append(jDoc, '\n')

	if pos != -1 {
		if pos < HeaderSize {
			return 0, fmt.Errorf("invalid pos")
		}
		_, err = f.Seek(pos, io.SeekStart)
		if err != nil {
			return 0, err
		}
	} else {
		sz, err := f.Seek(0, io.SeekEnd)
		if err != nil {
			return 0, err
		}
		if sz < HeaderSize {
			return 0, fmt.Errorf("invalid file")
		}
	}

	n, err := f.Write(jDoc)
	if err != nil {
		return 0, err
	}
	if n != len(jDoc) {
		return 0, fmt.Errorf("write size unexpected")
	}
	return int64(n), nil
}

func (l *JsonLog) ReadEntry(f File, pos int64, entry *LogEntry) (int64, error) {
	_, err := f.Seek(pos, io.SeekStart)
	if err != nil {
		return 0, err
	}

	reader := bufio.NewReader(f)
	jDoc, err := reader.ReadBytes('\n')
	if err != nil {
		return 0, err
	}
	readSz := len(jDoc)
	// won't be 0
	if len(jDoc) <= 1 {
		entry.Reset()
		return int64(readSz), nil
	}
	jDoc = jDoc[:len(jDoc)-1]
	err = json.Unmarshal(jDoc, entry)
	if err != nil {
		return 0, err
	}
	return int64(readSz), nil
}

func _() {
	var _ LogFormat = &JsonLog{}
}
