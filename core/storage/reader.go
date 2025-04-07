package storage

// Reader provides data reading operations
type Reader interface {
	Next() bool
	Values() map[string]interface{}
	Value(field string) interface{}
	Err() error
	Close() error
	Seek(offset int64) error
}
