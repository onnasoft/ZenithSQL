package columnstorage

import "github.com/onnasoft/ZenithSQL/core/storage"

type IDCursor struct {
	reader *ColumnReader
	ids    []int64
	index  int
	err    error
}

func NewIDCursor(reader *ColumnReader, ids []int64) *IDCursor {
	return &IDCursor{
		reader: reader,
		ids:    ids,
		index:  -1,
	}
}

func (c *IDCursor) Next() bool {
	if c.index+1 >= len(c.ids) {
		return false
	}
	c.index++
	id := c.ids[c.index]
	c.err = c.reader.Seek(id)
	return c.err == nil
}

func (c *IDCursor) Scan(dest map[string]interface{}) error {
	if c.err != nil {
		return c.err
	}
	values := c.reader.Values()
	for k, v := range values {
		dest[k] = v
	}
	return nil
}

func (c *IDCursor) ScanField(field string) interface{} {
	val, err := c.reader.GetValue(field)
	if err != nil {
		c.err = err
	}
	return val
}

func (c *IDCursor) FastScanField(col storage.ColumnData, value interface{}) (bool, error) {
	return c.reader.ReadFieldValue(col, value)
}

func (c *IDCursor) Err() error {
	return c.err
}

func (c *IDCursor) Close() error {
	return c.reader.Close()
}

func (c *IDCursor) Count() int64 {
	return int64(len(c.ids))
}

func (c *IDCursor) Limit(limit int64) {}
func (c *IDCursor) Skip(offset int64) {}
