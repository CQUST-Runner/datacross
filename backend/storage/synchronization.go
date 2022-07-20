package storage

import (
	"fmt"
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
	err = sqlite.Init(dbFilePath, "")
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

// 一个接口返回确认信息，一个接口清理wal
// 统一调用execLogOp
// currentValue的计算抽象
// sqlite作为initial
func doSync(p *Participant, info *NetworkInfo) error {
	found := false
	for _, other := range info.participants {
		if p.name == other {
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

	s := NodeStorageImpl{}
	s.Init()
	runner := LogRunner{}
	if err := runner.Init(p.name, &s); err != nil {
		return err
	}

	inputs := []*LogInput{}
	for _, v := range m {
		inputs = append(inputs, &LogInput{w: v.w, machineID: v.machineID, start: ""})
	}
	_, err := runner.Run(inputs...)
	if err != nil {
		return err
	}
	f := GrowOnlyForestImpl{}
	f.Init(&s)
	p.s.f = &f
	return nil
}
