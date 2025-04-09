package columnstorage

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/core/storage"
)

type ColumnData struct {
	*Column
	data []byte
}

type ColumnReader struct {
	columnsData map[string]*ColumnData
	current     int64
	stats       *storage.StorageStats
}

func NewColumnReader(columns map[string]*Column, stats *storage.StorageStats) storage.Reader {
	columnsData := make(map[string]*ColumnData, len(columns))
	for name, col := range columns {
		columnsData[name] = &ColumnData{
			Column: col,
			data:   col.MMapFile.Data(),
		}
	}

	return &ColumnReader{
		columnsData: columnsData,
		current:     -1,
		stats:       stats,
	}
}

func (r *ColumnReader) ColumnsData() map[string]*ColumnData {
	return r.columnsData
}

func (r *ColumnReader) Next() bool {
	if r.current+1 >= r.stats.TotalRows {
		return false
	}
	r.current++
	return true
}

func (r *ColumnReader) Seek(id int64) error {
	if id <= 0 || id > r.stats.TotalRows {
		return fmt.Errorf("invalid id: %d", id)
	}
	r.current = id - 1
	return nil
}

func (r *ColumnReader) Values() map[string]interface{} {
	values := make(map[string]interface{}, len(r.columnsData))
	for name, col := range r.columnsData {
		recordLength := col.Length + 2
		offset := r.current * int64(recordLength)
		data := col.data[offset : offset+int64(recordLength-1)]

		if data[0] != 1 {
			values[name] = nil
			continue
		}

		values[name] = col.parser(data[1:])
	}
	return values
}

func (r *ColumnReader) ReadFieldValue(col *ColumnData, value interface{}) error {
	if r.current < 0 || r.current >= r.stats.TotalRows {
		return fmt.Errorf("invalid current index: %d", r.current)
	}
	recordLength := col.Length + 2
	offset := r.current * int64(recordLength)

	data := col.data[offset : offset+int64(recordLength)]

	if data[0] != 1 {
		return nil
	}

	return col.read(data[1:], value)
}

func (r *ColumnReader) ReadValue(field string, value interface{}) error {
	col, ok := r.columnsData[field]
	if !ok {
		return fmt.Errorf("field %s not found", field)
	}

	recordLength := col.Length + 2
	offset := r.current * int64(recordLength)

	if r.current >= r.stats.TotalRows {
		return fmt.Errorf("invalid current index: %d", r.current)
	}

	data := col.data[offset : offset+int64(recordLength)]

	if data[0] != 1 {
		return nil
	}

	fmt.Println(data[0])
	fmt.Println(data[1:])

	return col.read(data[1:], value)
}

func (r *ColumnReader) GetValue(field string) (interface{}, error) {
	col, ok := r.columnsData[field]
	if !ok {
		return nil, fmt.Errorf("field %s not found", field)
	}

	recordLength := col.Length + 2
	offset := r.current * int64(recordLength)
	data := col.data[offset : offset+int64(recordLength)]

	if data[0] != 1 {
		return nil, nil
	}

	return col.parser(data[1:]), nil
}

func (r *ColumnReader) Close() error {
	return nil
}
