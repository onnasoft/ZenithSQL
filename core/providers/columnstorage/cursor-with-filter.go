package columnstorage

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/onnasoft/ZenithSQL/io/filters"
)

type ColumnCursorWithFilter struct {
	base   storage.Cursor
	filter *filters.Filter
	err    error
}

func NewColumnCursorWithFilter(cursor storage.Cursor, filter *filters.Filter) *ColumnCursorWithFilter {
	c := &ColumnCursorWithFilter{
		base:   cursor,
		filter: filter,
	}

	c.filter.Prepare(cursor.Reader().ScanMap())

	return c
}

func (c *ColumnCursorWithFilter) ColumnsData() map[string]storage.ColumnData {
	return c.base.ColumnsData()
}

func (c *ColumnCursorWithFilter) Next() bool {
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

func (c *ColumnCursorWithFilter) Scan(dest map[string]interface{}) error {
	return c.base.Scan(dest)
}

func (c *ColumnCursorWithFilter) ScanField(field string) interface{} {
	return c.base.ScanField(field)
}

func (c *ColumnCursorWithFilter) FastScanField(col storage.ColumnData, value interface{}) (bool, error) {
	return c.base.FastScanField(col, value)
}

func (c *ColumnCursorWithFilter) Err() error {
	if c.err != nil {
		return c.err
	}
	return c.base.Err()
}

func (c *ColumnCursorWithFilter) Close() error {
	return c.base.Close()
}

func (c *ColumnCursorWithFilter) Count() int64 {
	var count int64
	filter := c.filter

	for c.base.Next() {
		fmt.Println("Executing filter")
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

func (c *ColumnCursorWithFilter) Limit(limit int64) {
	c.base.Limit(limit)
}

func (c *ColumnCursorWithFilter) Skip(offset int64) {
	c.base.Skip(offset)
}

func (c *ColumnCursorWithFilter) Reader() storage.Reader {
	return c.base.Reader()
}
