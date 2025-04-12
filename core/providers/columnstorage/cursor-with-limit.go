package columnstorage

import (
	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/onnasoft/ZenithSQL/io/filters"
	"github.com/onnasoft/ZenithSQL/io/statement"
)

type columnCursorWithLimit struct {
	base     storage.Cursor
	limit    int64
	returned int64
}

func newColumnCursorWithLimit(base storage.Cursor, limit int64) (storage.Cursor, error) {
	return &columnCursorWithLimit{
		base:  base,
		limit: limit,
	}, nil
}

func (c *columnCursorWithLimit) ColumnsData() map[string]storage.ColumnData {
	return c.base.ColumnsData()
}

func (c *columnCursorWithLimit) Next() bool {
	if c.limit >= 0 && c.returned >= c.limit {
		return false
	}
	if c.base.Next() {
		c.returned++
		return true
	}
	return false
}

func (c *columnCursorWithLimit) Scan(dest map[string]interface{}) error {
	return c.base.Scan(dest)
}

func (c *columnCursorWithLimit) ScanField(field string) (interface{}, error) {
	return c.base.ScanField(field)
}

func (c *columnCursorWithLimit) FastScanField(col storage.ColumnData, value interface{}) (bool, error) {
	return c.base.FastScanField(col, value)
}

func (c *columnCursorWithLimit) Close() error {
	return c.base.Close()
}

func (c *columnCursorWithLimit) Count() (int64, error) {
	return -1, nil
}

func (c *columnCursorWithLimit) Reader() storage.Reader {
	return c.base.Reader()
}

func (c *columnCursorWithLimit) WithIDs(ids []int64) (storage.Cursor, error) {
	return c.base.WithIDs(ids)
}

func (c *columnCursorWithLimit) WithFilter(f *filters.Filter) (storage.Cursor, error) {
	return c.base.WithFilter(f)
}

func (c *columnCursorWithLimit) WithGroupBy(groupBy []string, aggs []statement.Aggregation) (storage.Cursor, error) {
	return c.base.WithGroupBy(groupBy, aggs)
}

func (c *columnCursorWithLimit) WithLimit(limit int64) (storage.Cursor, error) {
	return newColumnCursorWithLimit(c, limit)
}

func (c *columnCursorWithLimit) WithSkip(skip int64) (storage.Cursor, error) {
	return newColumnCursorWithSkip(c, skip)
}
