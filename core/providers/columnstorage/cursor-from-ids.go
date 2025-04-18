package columnstorage

import (
	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/onnasoft/ZenithSQL/io/filters"
	"github.com/onnasoft/ZenithSQL/io/statement"
)

type ColumnCursorFromIds struct {
	base  storage.Cursor
	ids   []int64
	index int
	limit int
	skip  int
	err   error
}

func newColumnCursorFromIds(cursor storage.Cursor, ids []int64) (*ColumnCursorFromIds, error) {
	return &ColumnCursorFromIds{
		base:  cursor,
		ids:   ids,
		index: -1,
		limit: -1, // -1 means no limit
	}, nil
}

func (c *ColumnCursorFromIds) ColumnsData() map[string]storage.ColumnData {
	return c.base.ColumnsData()
}

func (c *ColumnCursorFromIds) Next() bool {
	reader := c.base.Reader()
	for {
		c.index++
		if c.index >= len(c.ids) {
			return false
		}
		if c.index < c.skip {
			continue
		}
		if c.limit >= 0 && (c.index-c.skip) >= c.limit {
			return false
		}
		id := c.ids[c.index]
		c.err = reader.See(id)
		return c.err == nil
	}
}

func (c *ColumnCursorFromIds) Scan(dest map[string]interface{}) error {
	if c.err != nil {
		return c.err
	}
	reader := c.base.Reader()
	values := reader.Values()
	for k, v := range values {
		dest[k] = v
	}
	return nil
}

func (c *ColumnCursorFromIds) ScanField(field string) (interface{}, error) {
	return c.base.Reader().GetValue(field)
}

func (c *ColumnCursorFromIds) FastScanField(col storage.ColumnData, value interface{}) (bool, error) {
	return c.base.Reader().FastGetValue(col, value)
}

func (c *ColumnCursorFromIds) Err() error {
	return c.err
}

func (c *ColumnCursorFromIds) Close() error {
	return c.base.Reader().Close()
}

func (c *ColumnCursorFromIds) Count() (int64, error) {
	if c.limit >= 0 && c.skip < len(c.ids) {
		remaining := len(c.ids) - c.skip
		if remaining > c.limit {
			return int64(c.limit), nil
		}
		return int64(remaining), nil
	}
	if c.skip >= len(c.ids) {
		return 0, nil
	}
	return int64(len(c.ids) - c.skip), nil
}

func (c *ColumnCursorFromIds) Limit(limit int64) {
	c.limit = int(limit)
}

func (c *ColumnCursorFromIds) Skip(offset int64) {
	c.skip = int(offset)
}

func (c *ColumnCursorFromIds) Reader() storage.Reader {
	return c.base.Reader()
}

func (c *ColumnCursorFromIds) WithIDs(ids []int64) (storage.Cursor, error) {
	return newColumnCursorFromIds(c, ids)
}

func (c *ColumnCursorFromIds) WithFilter(filter *filters.Filter) (storage.Cursor, error) {
	return newColumnCursorWithFilter(c, filter)
}

func (c *ColumnCursorFromIds) WithGroupBy(groupBy []string, aggregations []statement.Aggregation) (storage.Cursor, error) {
	return newColumnCursorWithGroupBy(c, groupBy, aggregations)
}

func (c *ColumnCursorFromIds) WithLimit(limit int64) (storage.Cursor, error) {
	return newColumnCursorWithLimit(c, limit)
}

func (c *ColumnCursorFromIds) WithSkip(skip int64) (storage.Cursor, error) {
	return newColumnCursorWithSkip(c, skip)
}
