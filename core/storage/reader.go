package storage

// Reader provides data reading operations
type Reader interface {
	Next() bool
	ColumnsData() map[string]ColumnData
	Values() map[string]interface{}
	ReadValue(field string, value interface{}) error
	GetValue(field string) (interface{}, error)
	FastGetValue(col ColumnData, value interface{}) (bool, error)
	Close() error
	Seek(id int64) error
	CurrentID() int64
}
