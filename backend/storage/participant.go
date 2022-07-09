package storage

import (
	"fmt"
	"os"
	"path"
	"time"
)

const WalFileName = "0.wal"
const DBFileName = "0.db"
const SyncInterval = time.Minute

type CondenserLogEntry struct {
	Op    int32
	Key   string
	Value string
	Owner string
}

type LogCondenser struct {
	m               map[string]*CondenserLogEntry
	participantName string
}

func (l *LogCondenser) Init(participantName string) {
	l.m = make(map[string]*CondenserLogEntry)
	l.participantName = participantName
}

func (l *LogCondenser) WithCommitID(string) Storage {
	return l
}

func (l *LogCondenser) WithMachineID(string) Storage {
	return l
}

func (l *LogCondenser) Save(key string, value string) error {
	l.m[key] = &CondenserLogEntry{Op: int32(Op_Modify), Key: key, Value: value, Owner: l.participantName}
	return nil
}

func (l *LogCondenser) Del(key string) error {
	l.m[key] = &CondenserLogEntry{Op: int32(Op_Del), Key: key, Owner: l.participantName}
	return nil
}

func (l *LogCondenser) Has(key string) (bool, error) {
	return false, fmt.Errorf("unsupported")
}

func (l *LogCondenser) Load(key string) (string, error) {
	return "", fmt.Errorf("unsupported")
}

func (l *LogCondenser) All() ([][2]string, error) {
	return nil, fmt.Errorf("unsupported")
}

func (l *LogCondenser) Merge(Storage) error {
	return fmt.Errorf("unsupported")
}

func condenseParticipantLog(wd string, name string, lastSyncPos string) (string, map[string]*CondenserLogEntry, error) {
	p := path.Join(wd, name)
	walFile := path.Join(p, WalFileName)
	if !IsFile(walFile) {
		return "", nil, fmt.Errorf("wal not exist[%v]", walFile)
	}

	w := Wal{}
	err := w.Init(walFile, &BinLog{}, true)
	if err != nil {
		return "", nil, err
	}

	if w.header.LastEntryId == lastSyncPos {
		return w.header.LastEntryId, make(map[string]*CondenserLogEntry), nil
	}

	condenser := LogCondenser{}
	condenser.Init(name)
	if len(lastSyncPos) == 0 {
		err = w.Replay(&condenser, "")
	} else {
		err = w.Replay(&condenser, lastSyncPos)
	}
	if err != nil {
		return "", nil, err
	}
	return w.header.LastEntryId, condenser.m, nil
}

func collectChangesSinceLastSync(wd string, status *SyncStatus) (map[string][]*CondenserLogEntry, error) {
	result := map[string][]*CondenserLogEntry{}
	for name, pos := range status.Pos {
		newPos, log, err := condenseParticipantLog(wd, name, pos)
		if err != nil {
			return nil, err
		}
		for k, e := range log {
			result[k] = append(result[k], e)
		}
		status.Pos[name] = newPos
	}
	return result, nil
}

func discoveryOtherParticipants(wd string, name string) ([]string, error) {
	entries, err := os.ReadDir(wd)
	if err != nil {
		return nil, err
	}
	list := []string{}
	for _, e := range entries {
		if !e.IsDir() || e.Name() == name {
			continue
		}
		sign := path.Join(path.Join(wd, e.Name()), DBFileName)
		if IsFile(sign) {
			list = append(list, e.Name())
		}
	}
	return list, nil
}

type Participant struct {
	wd     string
	name   string
	others []string

	personalPath string
	walFile      string
	dbFile       string
	m            *MapWithWal
	sqlite       *SqliteAdapter
	s            *HybridStorage

	lastSyncTime time.Time
}

// TODO log format converter
// TODO command line db tool
// TODO background syncing, thread safety?
// TODO set json flag to output single line json
func (p *Participant) Init(wd string, name string) (err error) {
	if !path.IsAbs(wd) {
		cwd, err := os.Getwd()
		if err != nil {
			return err
		}
		wd = path.Join(cwd, wd)
	}
	wd = path.Clean(wd)

	personalPath := path.Join(wd, name)
	walFile := path.Join(personalPath, WalFileName)
	dbFile := path.Join(personalPath, DBFileName)

	if !IsDir(personalPath) {
		err := os.MkdirAll(personalPath, 0777)
		if err != nil {
			return err
		}
	}

	others, err := discoveryOtherParticipants(wd, name)
	if err != nil {
		return err
	}

	wal := Wal{}
	err = wal.Init(walFile, &BinLog{}, false)
	if err != nil {
		return err
	}
	m := &MapWithWal{}
	m.Init(&wal)
	defer func() {
		if err != nil {
			m.Close()
		}
	}()

	sqlite := SqliteAdapter{}
	// TODO set table name
	err = sqlite.Init(dbFile, "test", name)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			m.Close()
		}
	}()

	s := HybridStorage{}
	err = s.Init(m, &sqlite)
	if err != nil {
		return err
	}

	p.wd = wd
	p.name = name
	p.others = others
	p.personalPath = personalPath
	p.walFile = walFile
	p.dbFile = dbFile
	p.m = m
	p.sqlite = &sqlite
	p.s = &s
	p.lastSyncTime = time.Now().Add(-2 * SyncInterval)
	return nil
}

func (p *Participant) Close() {
	if p.s != nil {
		// it closes m and sqlite
		p.s.Close()
		p.s = nil
	}
}

func (p *Participant) trySync() {
	savedStatus, err := p.sqlite.LastSync()
	if err != nil {
		logger.Error("get last sync status failed[%v]", err)
		return
	}
	if savedStatus.Pos == nil {
		savedStatus.Pos = map[string]string{}
	}

	status := SyncStatus{Pos: make(map[string]string), Time: savedStatus.Time}
	for _, other := range p.others {
		if pos, ok := savedStatus.Pos[other]; ok {
			status.Pos[other] = pos
		} else {
			status.Pos[other] = ""
		}
	}
	if pos, ok := savedStatus.Pos[p.name]; ok {
		status.Pos[p.name] = pos
	} else {
		status.Pos[p.name] = ""
	}

	changes, err := collectChangesSinceLastSync(p.wd, &status)
	if err != nil {
		logger.Error("collect changes failed[%v]", err)
		return
	}
	executable := []*CondenserLogEntry{}
	for k, entries := range changes {
		if len(entries) == 1 {
			if entries[0].Owner != p.name {
				executable = append(executable, entries[0])
			}
		} else {
			logger.Warn("conflict changes on key[%v]", k)
		}
	}

	if len(executable) == 0 {
		logger.Info("no changes to be committed")
		return
	}
	err = p.sqlite.Transaction(func(s2 *SqliteAdapter) error {
		for _, entry := range executable {
			if entry == nil {
				continue
			}
			var e error
			switch entry.Op {
			case int32(Op_Modify):
				e = s2.WithMachineID(entry.Owner).WithCommitID("").Save(entry.Key, entry.Value)
			case int32(Op_Del):
				e = s2.WithMachineID(entry.Owner).WithCommitID("").Del(entry.Key)
			}
			if e != nil {
				return err
			}
		}

		return s2.SaveLastSync(savedStatus, &status)
	})
	if err != nil {
		logger.Error("merge change from others failed[%v]", err)
		return
	}
}

func (p *Participant) S() Storage {
	if time.Since(p.lastSyncTime) > SyncInterval {
		p.trySync()
		p.lastSyncTime = time.Now()
	}
	return p.s
}
