package columnstorage

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/onnasoft/ZenithSQL/io/filters"
	"github.com/onnasoft/ZenithSQL/io/statement"
	"github.com/onnasoft/ZenithSQL/model/aggregate"
)

type ColumnCursorWithGroupBy struct {
	base     storage.Cursor
	groupBy  []string
	agg      []statement.Aggregation
	aggFnMap map[string]aggregate.Aggregate
}

func newColumnCursorWithGroupBy(cursor storage.Cursor, groupBy []string, aggregation []statement.Aggregation) (*ColumnCursorWithGroupBy, error) {
	aggFnMap := make(map[string]aggregate.Aggregate)
	scanMap := cursor.Reader().ScanMap()
	for _, agg := range aggregation {
		scanner, ok := scanMap[agg.Column]
		if !ok {
			return nil, fmt.Errorf("column %s not found in cursor", agg.Column)
		}

		fn, err := aggregate.New(scanner.Type, agg.Function, scanner)
		if err != nil {
			return nil, fmt.Errorf("error creating aggregate function: %v", err)
		}
		aggFnMap[agg.Column] = fn
	}

	return &ColumnCursorWithGroupBy{
		base:     cursor,
		agg:      aggregation,
		groupBy:  groupBy,
		aggFnMap: aggFnMap,
	}, nil
}

func (c *ColumnCursorWithGroupBy) ColumnsData() map[string]storage.ColumnData {
	return c.base.ColumnsData()
}

func (c *ColumnCursorWithGroupBy) Next() bool {

	dataMap := make(map[string]interface{})

	for c.base.Next() {
		key := ""
		for _, col := range c.groupBy {
			val, err := c.base.ScanField(col)
			if err != nil {
				return false
			}
			if val == nil {
				continue
			}
			key += fmt.Sprintf("%v", val)
		}

		if key == "" {
			continue
		}

		if _, ok := dataMap[key]; !ok {
			dataMap[key] = make([]interface{}, 0, 1000)
		}

		record := c.base.Reader().Values()
		fmt.Println("record", record)
	}
	return false
}

func (c *ColumnCursorWithGroupBy) Scan(dest map[string]interface{}) error {
	return c.base.Scan(dest)
}

func (c *ColumnCursorWithGroupBy) ScanField(field string) (interface{}, error) {
	return c.base.ScanField(field)
}

func (c *ColumnCursorWithGroupBy) FastScanField(col storage.ColumnData, value interface{}) (bool, error) {
	return c.base.FastScanField(col, value)
}

func (c *ColumnCursorWithGroupBy) Close() error {
	return c.base.Close()
}

func (c *ColumnCursorWithGroupBy) Count() (int64, error) {
	return c.base.Count()
}

func (c *ColumnCursorWithGroupBy) Reader() storage.Reader {
	return c.base.Reader()
}

func (c *ColumnCursorWithGroupBy) WithIDs(ids []int64) (storage.Cursor, error) {
	return newColumnCursorFromIds(c, ids)
}

func (c *ColumnCursorWithGroupBy) WithFilter(filter *filters.Filter) (storage.Cursor, error) {
	return newColumnCursorWithFilter(c, filter)
}

func (c *ColumnCursorWithGroupBy) WithGroupBy(groupBy []string, aggregations []statement.Aggregation) (storage.Cursor, error) {
	return newColumnCursorWithGroupBy(c, groupBy, aggregations)
}

func (c *ColumnCursorWithGroupBy) WithLimit(limit int64) (storage.Cursor, error) {
	return newColumnCursorWithLimit(c, limit)
}

func (c *ColumnCursorWithGroupBy) WithSkip(skip int64) (storage.Cursor, error) {
	return newColumnCursorWithSkip(c, skip)
}
