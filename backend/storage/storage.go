package storage

import (
	"fmt"
	"strings"
)

type Value struct {
	key       string
	value     string
	machineID string
	gid       string
	seq       int
	branches  []*Value
}

func (v *Value) from(leaves []*DBRecord, machineID string) error {
	main := findMain(leaves, machineID)
	if main == nil {
		return fmt.Errorf("cannot find main node")
	}

	v.key = main.Key
	v.value = main.Value
	v.gid = main.CurrentLogGid
	v.machineID = main.MachineID
	v.seq = 0

	seq := 1
	for _, e := range leaves {
		if e == nil {
			continue
		}
		if e.CurrentLogGid == main.CurrentLogGid {
			continue
		}
		v.branches = append(v.branches,
			&Value{key: e.Key, value: e.Value,
				machineID: e.MachineID, gid: e.CurrentLogGid,
				seq: seq})
		seq++
	}
	return nil
}

func (v *Value) String() string {
	sb := strings.Builder{}
	sb.WriteString(v.value)
	nonEmpty := false
	for _, b := range v.branches {
		if b != nil {
			nonEmpty = true
			break
		}
	}
	if nonEmpty {
		sb.WriteString("(*)")
	}

	for _, b := range v.branches {
		if b == nil {
			continue
		}
		sb.WriteString(" ")
		sb.WriteString(b.value)
	}
	return sb.String()
}

// Storage ...
type Storage interface {
	Save(key string, value string) error
	Del(key string) error
	Has(key string) (bool, error)
	Load(key string) (val *Value, err error)
	All() ([]*Value, error)
	// Merge merges s into self, for duplicate keys, our side take precedence
	Merge(s Storage) error
}
