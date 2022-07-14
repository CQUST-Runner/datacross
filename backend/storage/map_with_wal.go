package storage

import (
	"fmt"
)

// MapWithWal ...
type MapWithWal struct {
	log      *Wal
	m        map[string]string
	commitID *string
}

func (s *MapWithWal) Init(log *Wal) {
	s.log = log
	s.m = map[string]string{}
}

func (s *MapWithWal) Close() error {
	if s.log != nil {
		err := s.log.Close()
		if err != nil {
			return err
		}
		s.log = nil
	}
	s.m = nil
	s.commitID = nil
	return nil
}

func (s *MapWithWal) WithCommitID(string) Storage {
	return s
}

func (s *MapWithWal) WithMachineID(string) Storage {
	return s
}

func (s *MapWithWal) Retrieve(commitID *string) *MapWithWal {
	return &MapWithWal{log: s.log, m: s.m, commitID: commitID}
}

func (s *MapWithWal) setCommitID(id string) {
	if s.commitID != nil {
		*s.commitID = id
	}
}

func (s *MapWithWal) LastEntryID() string {
	return s.log.header.LastEntryId
}

func (s *MapWithWal) Save(key string, value string) error {
	id, err := s.log.Append(int32(Op_Modify), key, value)
	if err != nil {
		return err
	}
	s.m[key] = value
	s.setCommitID(id)
	return nil
}

func (s *MapWithWal) Del(key string) error {
	id, err := s.log.Append(int32(Op_Del), key, "")
	if err != nil {
		return err
	}
	delete(s.m, key)
	s.setCommitID(id)
	return nil
}

func (s *MapWithWal) Has(key string) (bool, error) {
	if _, ok := s.m[key]; ok {
		return true, nil
	}
	return false, nil
}

func (s *MapWithWal) Load(key string) (string, error) {
	if val, ok := s.m[key]; ok {
		return val, nil
	}
	return "", fmt.Errorf("not exist")
}

func (s *MapWithWal) All() ([][2]string, error) {
	records := [][2]string{}
	for k, v := range s.m {
		records = append(records, [2]string{k, v})
	}
	return records, nil
}

func (s *MapWithWal) Merge(s2 Storage) error {
	records, err := s2.All()
	if err != nil {
		return err
	}

	for _, tuple := range records {
		key := tuple[0]
		value := tuple[1]
		if _, ok := s.m[key]; !ok {
			s.m[key] = value
		}
	}
	return nil
}

func (s *MapWithWal) Discard(key string, gids []string) error {
	return fmt.Errorf("unsupported")
}

func _() {
	var _ Storage = &MapWithWal{}
}
