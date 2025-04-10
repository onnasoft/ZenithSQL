package storage

import "github.com/onnasoft/ZenithSQL/model/types"

type ScanFunc func(value interface{}) (bool, error)

type ColumnScanner struct {
	Type     types.DataType // o el tipo que estés usando, como types.DataType
	Scan     ScanFunc
	Nullable bool // opcional si te sirve
}

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
	ScanMap() map[string]*ColumnScanner
}
