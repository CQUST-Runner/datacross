package storage

import (
	"os"
	"path"
	"time"
)

type NetworkInfo struct {
	wd           string
	participants []string
}

func (info *NetworkInfo) Others(self string) []string {
	others := []string{}
	for _, p := range info.participants {
		if p != self {
			others = append(others, p)
		}
	}
	return others
}

const WalFileName = "0.wal"
const DBFileName = "0.db"
const SyncInterval = time.Minute

func discoveryAllParticipants(wd string) ([]string, error) {
	entries, err := os.ReadDir(wd)
	if err != nil {
		return nil, err
	}
	list := []string{}
	for _, e := range entries {
		if !e.IsDir() {
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
	info *NetworkInfo
	name string

	personalPath string
	walFile      string
	dbFile       string
	s            *HybridStorage

	lastSyncTime time.Time
}

func getPersonalPath(wd string, pname string) string {
	personalPath := path.Join(wd, pname)
	return personalPath
}

func getWalFilePath(personalPath string) string {
	walFile := path.Join(personalPath, WalFileName)
	return walFile
}

func getDBFilePath(personalPath string) string {
	dbFile := path.Join(personalPath, DBFileName)
	return dbFile
}

// TODO log format converter
// TODO command line db tool
// TODO background syncing, thread safety?
// TODO set json flag to output single line json
func (p *Participant) Init(wd string, name string) (err error) {
	if !path.IsAbs(wd) && !(len(wd) > 1 && wd[1] == ':') {
		cwd, err := os.Getwd()
		if err != nil {
			return err
		}
		wd = path.Join(cwd, wd)
	}
	wd = path.Clean(wd)

	personalPath := getPersonalPath(wd, name)
	walFile := getWalFilePath(personalPath)
	dbFile := getDBFilePath(personalPath)

	if !IsDir(personalPath) {
		err := os.MkdirAll(personalPath, 0777)
		if err != nil {
			return err
		}
	}

	all, err := discoveryAllParticipants(wd)
	if err != nil {
		return err
	}

	wal := Wal{}
	err = wal.Init(walFile, &BinLog{}, false)
	if err != nil {
		return err
	}

	s := HybridStorage{}
	// err = s.Init(m, &sqlite)
	if err != nil {
		return err
	}

	p.info = &NetworkInfo{wd: wd, participants: all}
	p.name = name
	p.personalPath = personalPath
	p.walFile = walFile
	p.dbFile = dbFile
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
}

func (p *Participant) S() Storage {
	if time.Since(p.lastSyncTime) > SyncInterval {
		p.trySync()
		p.lastSyncTime = time.Now()
	}
	return p.s
}
