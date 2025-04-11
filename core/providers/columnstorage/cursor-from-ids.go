package columnstorage

import "github.com/onnasoft/ZenithSQL/core/storage"

type ColumnCursorFromIds struct {
	reader *ColumnReader
	ids    []int64
	index  int
	limit  int
	skip   int
	err    error
}

func NewColumnCursorFromIds(reader *ColumnReader, ids []int64) *ColumnCursorFromIds {
	return &ColumnCursorFromIds{
		reader: reader,
		ids:    ids,
		index:  -1,
		limit:  -1, // -1 means no limit
	}
}

func (c *ColumnCursorFromIds) ColumnsData() map[string]storage.ColumnData {
	return c.reader.ColumnsData()
}

func (c *ColumnCursorFromIds) Next() bool {
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
		c.err = c.reader.See(id)
		return c.err == nil
	}
}

func (c *ColumnCursorFromIds) Scan(dest map[string]interface{}) error {
	if c.err != nil {
		return c.err
	}
	values := c.reader.Values()
	for k, v := range values {
		dest[k] = v
	}
	return nil
}

func (c *ColumnCursorFromIds) ScanField(field string) (interface{}, error) {
	return c.reader.GetValue(field)
}

func (c *ColumnCursorFromIds) FastScanField(col storage.ColumnData, value interface{}) (bool, error) {
	return c.reader.FastGetValue(col, value)
}

func (c *ColumnCursorFromIds) Err() error {
	return c.err
}

func (c *ColumnCursorFromIds) Close() error {
	return c.reader.Close()
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
	return c.reader
}
