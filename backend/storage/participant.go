package storage

import (
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

func (p *ParticipantInfo) Init(wd string, name string, n *NetworkInfo2) error {
	personalPath := getPersonalPath(wd, name)
	walPath := getWalFilePath(personalPath)
	dbPath := getDBFilePath(personalPath)

	if !IsDir(personalPath) {
		err := os.MkdirAll(personalPath, 0777)
		if err != nil {
			return err
		}
	}

	p.name = name
	p.personalPath = personalPath
	p.walFile = walPath
	p.dbFile = dbPath
	p.network = n
	return nil
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
		_ = p.Init(wd, name, n)
		participants[name] = &p
	}

	n.wd = wd
	n.participants = participants

	return nil
}

func (n *NetworkInfo2) Add(name string) *ParticipantInfo {
	if _, ok := n.participants[name]; !ok {
		p := ParticipantInfo{}
		// TODO: handle error
		_ = p.Init(n.wd, name, n)
		n.participants[name] = &p

	}
	return n.participants[name]
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
