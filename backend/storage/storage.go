package storage

// Storage ...
type Storage interface {
	Save(key string, value string) error
	Del(key string) error
	Has(key string) (bool, error)
	Load(key string) (val string, err error)
	All() ([][2]string, error)
}
