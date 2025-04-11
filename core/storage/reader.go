package storage

import "github.com/onnasoft/ZenithSQL/core/buffer"

// Reader provides data reading operations
type Reader interface {
	Next() bool
	ColumnsData() map[string]ColumnData
	Values() map[string]interface{}
	ReadValue(field string, value interface{}) error
	GetValue(field string) (interface{}, error)
	FastGetValue(col ColumnData, value interface{}) (bool, error)
	Close() error
	See(id int64) error
	CurrentID() int64
	ScanMap() map[string]*buffer.Scanner
}
