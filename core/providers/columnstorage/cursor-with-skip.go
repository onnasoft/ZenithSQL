package columnstorage

import (
	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/onnasoft/ZenithSQL/io/filters"
	"github.com/onnasoft/ZenithSQL/io/statement"
)

type columnCursorWithSkip struct {
	base    storage.Cursor
	skip    int64
	skipped int64
}

func newColumnCursorWithSkip(base storage.Cursor, skip int64) (storage.Cursor, error) {
	return &columnCursorWithSkip{
		base: base,
		skip: skip,
	}, nil
}

func (c *columnCursorWithSkip) ColumnsData() map[string]storage.ColumnData {
	return c.base.ColumnsData()
}

func (c *columnCursorWithSkip) Next() bool {
	for c.skipped < c.skip {
		if !c.base.Next() {
			return false
		}
		c.skipped++
	}
	return c.base.Next()
}

func (c *columnCursorWithSkip) Scan(dest map[string]interface{}) error {
	return c.base.Scan(dest)
}

func (c *columnCursorWithSkip) ScanField(field string) (interface{}, error) {
	return c.base.ScanField(field)
}

func (c *columnCursorWithSkip) FastScanField(col storage.ColumnData, value interface{}) (bool, error) {
	return c.base.FastScanField(col, value)
}

func (c *columnCursorWithSkip) Close() error {
	return c.base.Close()
}

func (c *columnCursorWithSkip) Count() (int64, error) {
	return -1, nil
}

func (c *columnCursorWithSkip) Reader() storage.Reader {
	return c.base.Reader()
}

func (c *columnCursorWithSkip) WithIDs(ids []int64) (storage.Cursor, error) {
	return c.base.WithIDs(ids)
}

func (c *columnCursorWithSkip) WithFilter(f *filters.Filter) (storage.Cursor, error) {
	return c.base.WithFilter(f)
}

func (c *columnCursorWithSkip) WithGroupBy(groupBy []string, aggs []statement.Aggregation) (storage.Cursor, error) {
	return c.base.WithGroupBy(groupBy, aggs)
}

func (c *columnCursorWithSkip) WithLimit(limit int64) (storage.Cursor, error) {
	return newColumnCursorWithLimit(c, limit)
}

func (c *columnCursorWithSkip) WithSkip(skip int64) (storage.Cursor, error) {
	return newColumnCursorWithSkip(c, skip)
}
