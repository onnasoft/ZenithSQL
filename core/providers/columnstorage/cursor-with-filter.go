package columnstorage

import (
	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/onnasoft/ZenithSQL/io/filters"
	"github.com/onnasoft/ZenithSQL/io/statement"
)

type ColumnCursorWithFilter struct {
	base   storage.Cursor
	filter *filters.Filter
}

func newColumnCursorWithFilter(cursor storage.Cursor, filter *filters.Filter) (*ColumnCursorWithFilter, error) {
	c := &ColumnCursorWithFilter{
		base:   cursor,
		filter: filter,
	}

	c.filter.Prepare(cursor.Reader().ScanMap())

	return c, nil
}

func (c *ColumnCursorWithFilter) ColumnsData() map[string]storage.ColumnData {
	return c.base.ColumnsData()
}

func (c *ColumnCursorWithFilter) Next() bool {
	for c.base.Next() {
		ok, err := c.filter.Execute()
		if err != nil {
			return false
		}
		if ok {
			return true
		}
	}

	return false
}

func (c *ColumnCursorWithFilter) Scan(dest map[string]interface{}) error {
	return c.base.Scan(dest)
}

func (c *ColumnCursorWithFilter) ScanField(field string) (interface{}, error) {
	return c.base.ScanField(field)
}

func (c *ColumnCursorWithFilter) FastScanField(col storage.ColumnData, value interface{}) (bool, error) {
	return c.base.FastScanField(col, value)
}

func (c *ColumnCursorWithFilter) Close() error {
	return c.base.Close()
}

func (c *ColumnCursorWithFilter) Count() (int64, error) {
	var count int64
	filter := c.filter

	for c.base.Next() {
		ok, err := filter.Execute()
		if err != nil {
			return 0, err
		}
		if ok {
			count++
		}
	}
	return count, nil
}

func (c *ColumnCursorWithFilter) Reader() storage.Reader {
	return c.base.Reader()
}

func (c *ColumnCursorWithFilter) WithIDs(ids []int64) (storage.Cursor, error) {
	return newColumnCursorFromIds(c, ids)
}

func (c *ColumnCursorWithFilter) WithFilter(filter *filters.Filter) (storage.Cursor, error) {
	return newColumnCursorWithFilter(c, filter)
}

func (c *ColumnCursorWithFilter) WithGroupBy(groupBy []string, aggregations []statement.Aggregation) (storage.Cursor, error) {
	return newColumnCursorWithGroupBy(c, groupBy, aggregations)
}

func (c *ColumnCursorWithFilter) WithLimit(limit int64) (storage.Cursor, error) {
	return newColumnCursorWithLimit(c, limit)
}

func (c *ColumnCursorWithFilter) WithSkip(skip int64) (storage.Cursor, error) {
	return newColumnCursorWithSkip(c, skip)
}
