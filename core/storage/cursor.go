package storage

// Cursor provides query result iteration
type Cursor interface {
	Next() bool
	Scan(dest map[string]interface{}) error
	ScanField(field string) interface{}
	FastScanField(col ColumnData, value interface{}) (bool, error)
	Err() error
	Close() error
	Count() int64
	Limit(limit int64)
	Skip(offset int64)
}
