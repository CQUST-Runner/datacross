package storage

// Storage ...
type Storage interface {
	Save(key string, value string) error
	Del(key string) error
	Has(key string) (bool, error)
	Load(key string) (val string, err error)
	All() ([][2]string, error)
	// WithCommitID able to associate an id for the following operation
	WithCommitID(commitID string) Storage
	// WithMachineID operate data specifically belongs to a machine
	WithMachineID(machineID string) Storage
	// Merge merges s into self, for duplicate keys, our side take precedence
	Merge(s Storage) error
}
