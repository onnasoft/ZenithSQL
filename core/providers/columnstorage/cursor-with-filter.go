package columnstorage

import (
	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/onnasoft/ZenithSQL/io/filters"
)

type CursorWithFilter struct {
	base   storage.Cursor
	filter *filters.Filter
	err    error
}

func NewCursorWithFilter(cursor storage.Cursor, filter *filters.Filter) *CursorWithFilter {
	c := &CursorWithFilter{
		base:   cursor,
		filter: filter,
	}

	c.filter.Prepare(cursor.Reader().ScanMap())

	return c
}

func (c *CursorWithFilter) ColumnsData() map[string]storage.ColumnData {
	return c.base.ColumnsData()
}

func (c *CursorWithFilter) Next() bool {
	for c.base.Next() {
		ok, err := c.filter.Execute()
		if err != nil {
			c.err = err
			return false
		}
		if ok {
			return true
		}
	}

	return false
}

func (c *CursorWithFilter) Scan(dest map[string]interface{}) error {
	return c.base.Scan(dest)
}

func (c *CursorWithFilter) ScanField(field string) interface{} {
	return c.base.ScanField(field)
}

func (c *CursorWithFilter) FastScanField(col storage.ColumnData, value interface{}) (bool, error) {
	return c.base.FastScanField(col, value)
}

func (c *CursorWithFilter) Err() error {
	if c.err != nil {
		return c.err
	}
	return c.base.Err()
}

func (c *CursorWithFilter) Close() error {
	return c.base.Close()
}

func (c *CursorWithFilter) Count() int64 {
	var count int64
	filter := c.filter

	for c.base.Next() {
		ok, err := filter.Execute()
		if err != nil {
			c.err = err
			break
		}
		if ok {
			count++
		}
	}
	return count
}

func (c *CursorWithFilter) Limit(limit int64) {
	c.base.Limit(limit)
}

func (c *CursorWithFilter) Skip(offset int64) {
	c.base.Skip(offset)
}

func (c *CursorWithFilter) Reader() storage.Reader {
	return c.base.Reader()
}
