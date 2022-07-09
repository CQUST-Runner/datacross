package storage

import "fmt"

// HybridStorage ...
type HybridStorage struct {
	m      *MapWithWal
	sqlite *SqliteAdapter
}

func (s *HybridStorage) keepUpWithLog() bool {
	// TODO: cache them?
	lastEntryID := s.m.LastEntryID()
	lastCommit, err := s.sqlite.LastCommit()
	if err != nil {
		logger.Error("get last commit failed[%v]", err)
		return false
	}
	if lastEntryID != lastCommit {
		if len(lastCommit) == 0 {
			// make it explicitly start from beginning
			err = s.m.log.Replay(s.sqlite, "")
		} else {
			err = s.m.log.Replay(s.sqlite, lastCommit)
		}
		if err != nil {
			logger.Error("replay failed[%v]", err)
			return false
		}
	}
	lastCommit, err = s.sqlite.LastCommit()
	if err != nil {
		logger.Error("get last commit again failed[%v]", err)
		return false
	}
	logger.Info("last entry id[%v], last commit[%v]", lastEntryID, lastCommit)
	return lastCommit == lastEntryID
}

func (s *HybridStorage) Init(m *MapWithWal, sqlite *SqliteAdapter) error {
	s.m = m
	s.sqlite = sqlite

	ok := s.keepUpWithLog()
	if !ok {
		logger.Warn("sqlite is lag behind from wal")
	}

	return nil
}

func (s *HybridStorage) Close() {
	if s.m != nil {
		err := s.m.Close()
		if err != nil {
			logger.Error("close map failed[%v]", err)
		}
		s.m = nil
	}
	if s.sqlite != nil {
		err := s.sqlite.Close()
		if err != nil {
			logger.Error("close sqlite failed[%v]", err)
		}
		s.sqlite = nil
	}
}

func (s *HybridStorage) WithCommitID(string) Storage {
	return s
}

func (s *HybridStorage) WithMachineID(string) Storage {
	return s
}

func (s *HybridStorage) Save(key string, value string) error {
	ok := s.keepUpWithLog()
	if !ok {
		logger.Warn("sqlite is lag behind from wal")
	}

	var commitID string
	err := s.m.Retrieve(&commitID).Save(key, value)
	if err != nil {
		return err
	}

	if !ok {
		return nil
	}
	err = s.sqlite.WithCommitID(commitID).Save(key, value)
	if err != nil {
		logger.Warn("save key[%v] value[%v] into sqlite failed", key, value)
	}
	return nil
}

func (s *HybridStorage) Del(key string) error {
	ok := s.keepUpWithLog()
	if !ok {
		logger.Warn("sqlite is lag behind from wal")
	}

	var commitID string
	err := s.m.Retrieve(&commitID).Del(key)
	if err != nil {
		return err
	}

	if !ok {
		return nil
	}
	err = s.sqlite.WithCommitID(commitID).Del(key)
	if err != nil {
		logger.Warn("del key[%v] from sqlite failed", key)
	}

	return nil
}

func (s *HybridStorage) Has(key string) (bool, error) {
	has, err := s.m.Has(key)
	if err != nil {
		return false, err
	}
	if has {
		return true, nil
	}

	return s.sqlite.Has(key)
}

func (s *HybridStorage) Load(key string) (string, error) {
	val, err := s.m.Load(key)
	if err == nil {
		return val, nil
	}
	return s.sqlite.Load(key)
}

func (s *HybridStorage) All() ([][2]string, error) {
	err := s.m.Merge(s.sqlite)
	if err != nil {
		return nil, err
	}
	return s.m.All()
}

func (s *HybridStorage) Merge(Storage) error {
	return fmt.Errorf("unsupported")
}

func _() {
	var _ Storage = &HybridStorage{}
}
