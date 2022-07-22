package storage

type Value struct {
	key       string
	value     string
	machineID string
	gid       string
	branches  []*Value
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
