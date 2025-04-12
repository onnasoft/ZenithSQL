package storage

import (
	"github.com/onnasoft/ZenithSQL/io/filters"
	"github.com/onnasoft/ZenithSQL/io/statement"
)

// Cursor provides query result iteration
type Cursor interface {
	ColumnsData() map[string]ColumnData
	Next() bool
	Scan(dest map[string]interface{}) error
	ScanField(field string) (interface{}, error)
	FastScanField(col ColumnData, value interface{}) (bool, error)
	Close() error
	Count() (int64, error)
	Reader() Reader
	WithIDs(ids []int64) (Cursor, error)
	WithFilter(filter *filters.Filter) (Cursor, error)
	WithGroupBy(groupBy []string, aggregations []statement.Aggregation) (Cursor, error)
	WithLimit(limit int64) (Cursor, error)
	WithSkip(skip int64) (Cursor, error)
}
