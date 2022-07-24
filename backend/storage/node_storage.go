package storage

import (
	"container/list"
	"fmt"
)

type ReadOnlyNodeStorage interface {
	GetByKey(key string) ([]*DBRecord, error)
	GetByGid(gid string) (*DBRecord, error)
	AllNodes() ([]*DBRecord, error)
}

type NodeStorage interface {
	ReadOnlyNodeStorage
	Add(record *DBRecord) error
	Replace(old string, new *DBRecord) error
	Merge(other ReadOnlyNodeStorage) error
}

type NodeStorageImpl struct {
	l *list.List

	keyIndex map[string]*list.List
	gidIndex map[string]*list.Element
}

func (n *NodeStorageImpl) Init() {
	n.clear()
}

func (n *NodeStorageImpl) clear() {
	n.l = list.New()
	n.keyIndex = make(map[string]*list.List)
	n.gidIndex = make(map[string]*list.Element)
}

func (n *NodeStorageImpl) getByGidInternal(gid string) *list.Element {
	e, ok := n.gidIndex[gid]
	if !ok {
		return nil
	}
	return e
}

func (n *NodeStorageImpl) addNodeInternal(record *DBRecord) error {
	result := n.getByGidInternal(record.CurrentLogGid)
	if result != nil {
		return fmt.Errorf("node exists")
	}
	e := n.l.PushBack(record)
	elements, ok := n.keyIndex[record.Key]
	if !ok {
		elements = list.New()
		n.keyIndex[record.Key] = elements
	}
	elements.PushBack(e)
	n.gidIndex[record.CurrentLogGid] = e
	return nil
}

// make sure e is not nil and e is in n.l
func (n *NodeStorageImpl) delNodeInternal(e *list.Element) {
	record := n.l.Remove(e).(*DBRecord)
	delete(n.gidIndex, record.CurrentLogGid)
	elements, ok := n.keyIndex[record.Key]
	if ok {
		for e1 := elements.Front(); e1 != nil; e1 = e1.Next() {
			if e1.Value.(*list.Element).Value.(*DBRecord).CurrentLogGid == record.CurrentLogGid {
				elements.Remove(e1)
				break
			}
		}
	}
}

func (n *NodeStorageImpl) GetByKey(key string) ([]*DBRecord, error) {
	elements, ok := n.keyIndex[key]
	if !ok {
		return nil, nil
	}

	records := make([]*DBRecord, elements.Len())
	for e := elements.Front(); e != nil; e = e.Next() {
		record := e.Value.(*list.Element).Value.(*DBRecord)
		records = append(records, record)
	}
	return records, nil
}

func (n *NodeStorageImpl) GetByGid(gid string) (*DBRecord, error) {
	result := n.getByGidInternal(gid)
	if result != nil {
		return result.Value.(*DBRecord), nil
	}
	return nil, nil
}

func (n *NodeStorageImpl) Add(record *DBRecord) error {
	return n.addNodeInternal(record)
}

func (n *NodeStorageImpl) Replace(old string, new *DBRecord) error {
	e := n.getByGidInternal(old)
	if e == nil {
		return fmt.Errorf("old not exist")
	}

	e1 := n.getByGidInternal(new.CurrentLogGid)
	if e1 != nil {
		return fmt.Errorf("new node already exist")
	}

	if e.Value.(*DBRecord).Key != new.Key {
		return fmt.Errorf("key not match")
	}

	if err := n.addNodeInternal(new); err != nil {
		return err
	}
	n.delNodeInternal(e)
	return nil
}

func (n *NodeStorageImpl) del(gid string) error {
	e := n.getByGidInternal(gid)
	if e == nil {
		return nil
	}
	n.delNodeInternal(e)
	return nil
}

func (n *NodeStorageImpl) AllNodes() ([]*DBRecord, error) {
	results := make([]*DBRecord, 0, n.l.Len())
	for e := n.l.Front(); e != nil; e = e.Next() {
		results = append(results, e.Value.(*DBRecord))
	}
	return results, nil
}

func (n *NodeStorageImpl) Merge(other ReadOnlyNodeStorage) error {
	all, err := other.AllNodes()
	if err != nil {
		return err
	}

	for _, node := range all {
		if node == nil {
			continue
		}
		_ = n.Add(node)
	}
	return nil
}
