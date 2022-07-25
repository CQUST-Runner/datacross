package storage

import (
	"fmt"
	"strings"
)

// suppose Visible()==true
func compareNode(a *DBRecord, b *DBRecord, machineID string) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	if a.Changes(machineID) > b.Changes(machineID) {
		return 1
	} else if a.Changes(machineID) < b.Changes(machineID) {
		return -1
	}

	if a.Seq > b.Seq {
		return 1
	} else if a.Seq < b.Seq {
		return -1
	}

	if a.MachineID > b.MachineID {
		return 1
	} else if a.MachineID < b.MachineID {
		return -1
	} else {
		// should not be
		return 0
	}
}

func findMain(a []*DBRecord, machineID string) *DBRecord {
	var maxRecord *DBRecord
	for _, record := range a {
		if compareNode(record, maxRecord, machineID) > 0 {
			maxRecord = record
		}
	}
	return maxRecord
}

type ValueVersion struct {
	key       string
	value     string
	machineID string
	gid       string
	seq       int
}

func (v *ValueVersion) String() string {
	return fmt.Sprintf("%v\t%v\t%v\t%v", v.key, v.value, v.machineID, v.seq)
}

type Value struct {
	versions []*ValueVersion
}

func (v *Value) setMain(key string, value string) {
	main := ValueVersion{key: key, value: value, seq: 0}
	if len(v.versions) > 0 {
		v.versions[0] = &main
	} else {
		v.versions = append(v.versions, &main)
	}
}

func (v *Value) from(leaves []*DBRecord, machineID string) error {
	main := findMain(leaves, machineID)
	if main == nil {
		return fmt.Errorf("cannot find main node")
	}

	v.versions = append(v.versions,
		&ValueVersion{key: main.Key, value: main.Value,
			gid: main.CurrentLogGid, machineID: main.MachineID,
			seq: 0})

	seq := 1
	for _, e := range leaves {
		if e == nil {
			continue
		}
		if e.CurrentLogGid == main.CurrentLogGid {
			continue
		}
		v.versions = append(v.versions,
			&ValueVersion{key: e.Key, value: e.Value,
				machineID: e.MachineID, gid: e.CurrentLogGid,
				seq: seq})
		seq++
	}
	return nil
}

func (v *Value) ValidSeq(seq int) bool {
	return seq < len(v.versions)
}

func (v *Value) Versions() []*ValueVersion {
	return v.versions
}

func (v *Value) Branches() []*ValueVersion {
	return v.versions[1:]
}

func (v *Value) Main() *ValueVersion {
	return v.versions[0]
}

func (v *Value) String() string {
	sb := strings.Builder{}
	sb.WriteString(v.Main().key)
	sb.WriteString(": ")
	sb.WriteString(v.Main().value)
	nonEmpty := false
	for _, b := range v.Branches() {
		if b != nil {
			nonEmpty = true
			break
		}
	}
	if nonEmpty {
		sb.WriteString("(*)")
	}

	for _, b := range v.Branches() {
		if b == nil {
			continue
		}
		sb.WriteString(" ")
		sb.WriteString(b.value)
	}
	return sb.String()
}

func valuesToArray(v []*Value) [][2]string {
	results := [][2]string{}
	for _, vv := range v {
		if vv == nil {
			continue
		}
		results = append(results, [2]string{vv.Main().key, vv.Main().value})
	}
	return results
}

// Storage ...
type Storage interface {
	Save(key string, value string) error
	Del(key string) error
	Has(key string) (bool, error)
	Load(key string) (val *Value, err error)
	All() ([]*Value, error)
	Accept(v *Value, seq int) error
}
