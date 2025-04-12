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
	Limit(limit int64)
	Skip(offset int64)
	Reader() Reader
	WithIDs(ids []int64) (Cursor, error)
	WithFilter(filter *filters.Filter) (Cursor, error)
	WithAggregations(agg []statement.Aggregation) (Cursor, error)
}
