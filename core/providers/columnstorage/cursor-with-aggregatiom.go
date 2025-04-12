package columnstorage

import (
	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/onnasoft/ZenithSQL/io/filters"
	"github.com/onnasoft/ZenithSQL/io/statement"
)

type ColumnCursorWithAggregation struct {
	base     storage.Cursor
	agg      []statement.Aggregation
	limit    int64
	offset   int64
	skipped  int64
	returned int64
}

func newColumnCursorWithAggregations(cursor storage.Cursor, aggregation []statement.Aggregation) (*ColumnCursorWithAggregation, error) {
	return &ColumnCursorWithAggregation{
		base: cursor,
		agg:  aggregation,
	}, nil
}

func (c *ColumnCursorWithAggregation) ColumnsData() map[string]storage.ColumnData {
	return c.base.ColumnsData()
}

func (c *ColumnCursorWithAggregation) Next() bool {
	for c.base.Next() {
		if c.skipped < c.offset {
			c.skipped++
			continue
		}
		if c.limit > 0 && c.returned >= c.limit {
			return false
		}
		c.returned++
		return true
	}
	return false
}

func (c *ColumnCursorWithAggregation) Scan(dest map[string]interface{}) error {
	return c.base.Scan(dest)
}

func (c *ColumnCursorWithAggregation) ScanField(field string) (interface{}, error) {
	return c.base.ScanField(field)
}

func (c *ColumnCursorWithAggregation) FastScanField(col storage.ColumnData, value interface{}) (bool, error) {
	return c.base.FastScanField(col, value)
}

func (c *ColumnCursorWithAggregation) Close() error {
	return c.base.Close()
}

func (c *ColumnCursorWithAggregation) Count() (int64, error) {
	return c.base.Count()
}

func (c *ColumnCursorWithAggregation) Limit(limit int64) {
	c.limit = limit
}

func (c *ColumnCursorWithAggregation) Skip(offset int64) {
	c.offset = offset
}

func (c *ColumnCursorWithAggregation) Reader() storage.Reader {
	return c.base.Reader()
}

func (c *ColumnCursorWithAggregation) WithIDs(ids []int64) (storage.Cursor, error) {
	return newColumnCursorFromIds(c, ids)
}

func (c *ColumnCursorWithAggregation) WithFilter(filter *filters.Filter) (storage.Cursor, error) {
	return newColumnCursorWithFilter(c, filter)
}

func (c *ColumnCursorWithAggregation) WithAggregations(aggregations []statement.Aggregation) (storage.Cursor, error) {
	return newColumnCursorWithAggregations(c, aggregations)
}
