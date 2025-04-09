package storage

// Reader provides data reading operations
type Reader interface {
	Next() bool
	Values() map[string]interface{}
	ReadValue(field string, value interface{}) error
	GetValue(field string) (interface{}, error)
	Close() error
	Seek(id int64) error
}
