package storage

import (
	"fmt"

	gogoproto "github.com/gogo/protobuf/proto"
)

type SyncContext struct {
	machineID string
	status    *SyncStatus

	personalPath string
	walFilePath  string
	dbFilePath   string

	w      *Wal
	sqlite *SqliteAdapter
}

func (c *SyncContext) Init(info *NetworkInfo, pname string) (err error) {

	personalPath := getPersonalPath(info.wd, pname)
	dbFilePath := getDBFilePath(personalPath)
	walFilePath := getWalFilePath(personalPath)

	w := Wal{}
	err = w.Init(walFilePath, &BinLog{}, true)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			w.Close()
		}
	}()

	// TODO open for readonly?
	sqlite := SqliteAdapter{}
	err = sqlite.Init(dbFilePath, "", pname)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			sqlite.Close()
		}
	}()

	status, err := sqlite.LastSync()
	if err != nil {
		return err
	}

	c.machineID = pname
	c.dbFilePath = dbFilePath
	c.personalPath = personalPath
	c.walFilePath = walFilePath
	c.w = &w
	c.sqlite = &sqlite
	c.status = status
	return nil
}

func (c *SyncContext) Close() {
	if c.w != nil {
		err := c.w.Close()
		if err != nil {
			logger.Warn("close wal failed[%v]", err)
		}
		c.w = nil
	}
	if c.sqlite != nil {
		err := c.sqlite.Close()
		if err != nil {
			logger.Warn("close sqlite failed[%v]", err)
		}
		c.sqlite = nil
	}
}

type Patch struct {
}

func (p *Patch) Merge(p2 *Patch) error {
	return nil
}

func isLeading(w *Wal, id1 string, id2 string) (bool, error) {
	if len(id1) == 0 {
		return false, nil
	}
	if len(id2) == 0 {
		return true, nil
	}
	num := 1
	id1Num := 0
	id2Num := 0
	err := w.Foreach(func(entry *LogEntry) bool {
		if entry.Gid == id1 {
			id1Num = num
		} else if entry.Gid == id2 {
			id2Num = num
		}
		num++
		return id1Num == 0 || id2Num == 0
	})
	if err != nil {
		return false, err
	}
	return id1Num > id2Num, nil
}

type CondenserLogEntry2 struct {
	LogEntry
	MachineID string
}

type LogCondenser2 struct {
	m map[string][]*CondenserLogEntry2
}

func (c *LogCondenser2) Init() {
	c.m = make(map[string][]*CondenserLogEntry2)
}

func (c *LogCondenser2) Append(entry *CondenserLogEntry2) {

	arr, ok := c.m[entry.Key]
	if !ok {
		c.m[entry.Key] = append(c.m[entry.Key], entry)
		return
	}

	for i, e := range arr {
		if e.MachineID == entry.MachineID {
			arr[i] = entry
			return
		}
	}

	c.m[entry.Key] = append(c.m[entry.Key], entry)
}

// TODO save entry num in file header
func condenseLog(c *SyncContext, start string, end string, condenser *LogCondenser2) error {
	entries := []*LogEntry{}
	err := c.w.Range(start, end, func(entry *LogEntry) bool {
		entries = append(entries, entry)
		return true
	})
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry == nil {
			continue
		}

		entry2 := CondenserLogEntry2{MachineID: c.machineID}
		entry2.LogEntry = *gogoproto.Clone(entry).(*LogEntry)
		condenser.Append(&CondenserLogEntry2{})
	}
	return nil
}

func doSync(self string, info *NetworkInfo) error {
	found := false
	for _, other := range info.participants {
		if self == other {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("self not found in network info")
	}

	m := map[string]*SyncContext{}
	for _, p := range info.participants {
		c := SyncContext{}
		err := c.Init(info, p)
		if err != nil {
			return err
		}
		m[p] = &c
	}

	return nil
}
