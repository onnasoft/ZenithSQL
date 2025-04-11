package storage

// Cursor provides query result iteration
type Cursor interface {
	ColumnsData() map[string]ColumnData
	Next() bool
	Scan(dest map[string]interface{}) error
	ScanField(field string) (interface{}, error)
	FastScanField(col ColumnData, value interface{}) (bool, error)
	Close() error
	Count() (int64, error)
	Limit(limit int64)
	Skip(offset int64)
	Reader() Reader
}
