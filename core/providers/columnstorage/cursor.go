package columnstorage

import "github.com/onnasoft/ZenithSQL/core/storage"

type ColumnCursor struct {
	reader *ColumnReader
	err    error
	limit  int64
	skip   int64
	count  int64
}

func NewColumnCursor(reader *ColumnReader) *ColumnCursor {
	return &ColumnCursor{
		reader: reader,
		limit:  -1,
		skip:   0,
		count:  0,
	}
}

func (c *ColumnCursor) ColumnsData() map[string]storage.ColumnData {
	return c.reader.ColumnsData()
}

func (c *ColumnCursor) Next() bool {
	if c.err != nil {
		return false
	}

	// Skip rows
	for c.count < c.skip {
		if !c.reader.Next() {
			return false
		}
		c.count++
	}

	// Check limit
	if c.limit >= 0 && (c.count-c.skip) >= c.limit {
		return false
	}

	if !c.reader.Next() {
		return false
	}

	c.count++
	return true
}

func (c *ColumnCursor) Scan(dest map[string]interface{}) error {
	if c.err != nil {
		return c.err
	}

	values := c.reader.Values()
	for k, v := range values {
		dest[k] = v
	}

	return nil
}

func (c *ColumnCursor) ScanField(field string) interface{} {
	val, err := c.reader.GetValue(field)
	if err != nil {
		c.err = err
		return nil
	}
	return val
}

func (c *ColumnCursor) FastScanField(col storage.ColumnData, value interface{}) (bool, error) {
	return c.reader.FastGetValue(col, value)
}

func (c *ColumnCursor) Err() error {
	return c.err
}

func (c *ColumnCursor) Close() error {
	return c.reader.Close()
}

func (c *ColumnCursor) Count() int64 {
	return c.count - c.skip
}

func (c *ColumnCursor) Limit(limit int64) {
	c.limit = limit
}

func (c *ColumnCursor) Skip(offset int64) {
	c.skip = offset
}

func (c *ColumnCursor) Reader() storage.Reader {
	return c.reader
}
