package storage

import (
	"fmt"
	"os"
	"path"
	"time"
)

type ParticipantInfo struct {
	name string

	personalPath string
	walFile      string
	dbFile       string

	network *NetworkInfo2
}

func (p *ParticipantInfo) Init(wd string, name string, n *NetworkInfo2) {
	personalPath := getPersonalPath(wd, name)
	walPath := getWalFilePath(personalPath)
	dbPath := getDBFilePath(personalPath)

	p.name = name
	p.personalPath = personalPath
	p.walFile = walPath
	p.dbFile = dbPath
	p.network = n
}

type NetworkInfo2 struct {
	wd           string
	participants map[string]*ParticipantInfo
}

func (n *NetworkInfo2) Init(wd string) error {
	all, err := discoveryAllParticipants(wd)
	if err != nil {
		return err
	}
	participants := map[string]*ParticipantInfo{}
	for _, name := range all {
		p := ParticipantInfo{}
		p.Init(wd, name, n)
		participants[name] = &p
	}

	n.wd = wd
	n.participants = participants

	return nil
}

func (n *NetworkInfo2) Add(name string) *ParticipantInfo {
	if _, ok := n.participants[name]; !ok {
		p := ParticipantInfo{}
		p.Init(n.wd, name, n)
		n.participants[name] = &p
	}
	return n.participants[name]
}

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
		sign := path.Join(path.Join(wd, e.Name()), WalFileName)
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

	wal := Wal{}
	err = wal.Init(walFile, &BinLog{}, false)
	if err != nil {
		return err
	}

	s := HybridStorage{}
	err = s.Init(wd, name)
	if err != nil {
		return err
	}

	all, err := discoveryAllParticipants(wd)
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
		p.s.Close()
		p.s = nil
	}
}

func (p *Participant) trySync() {
	err := doSync(p, p.info)
	if err != nil {
		fmt.Println(err)
	}
}

func (p *Participant) S() Storage {
	if time.Since(p.lastSyncTime) > SyncInterval {
		p.trySync()
		p.lastSyncTime = time.Now()
	}
	return p.s
}
