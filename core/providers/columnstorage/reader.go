package columnstorage

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/core/buffer"
	"github.com/onnasoft/ZenithSQL/core/storage"
)

type ColumnReader struct {
	columnsData map[string]*ColumnData
	current     int64
	totalRows   int64
}

func NewColumnReader(columns map[string]*Column, stats *storage.StorageStats) (*ColumnReader, error) {
	columnsData := make(map[string]*ColumnData, len(columns))
	for name, col := range columns {
		data, err := col.AllocateView()
		if err != nil {
			return nil, fmt.Errorf("failed to allocate view for column %s: %w", name, err)
		}
		columnsData[name] = &ColumnData{
			Column: col,
			data:   data,
		}
	}

	return &ColumnReader{
		current:     -1,
		totalRows:   stats.TotalRows,
		columnsData: columnsData,
	}, nil
}

func (r *ColumnReader) ColumnsData() map[string]storage.ColumnData {
	result := make(map[string]storage.ColumnData, len(r.columnsData))
	for name, col := range r.columnsData {
		result[name] = col
	}
	return result
}

func (r *ColumnReader) Next() bool {
	if r.current+1 >= r.totalRows {
		return false
	}
	r.current++
	return true
}

func (r *ColumnReader) See(id int64) error {
	if id <= 0 || id > r.totalRows {
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

		values[name] = col.DataType.Parse(data[1:])
	}
	return values
}

func (r *ColumnReader) FastGetValue(col storage.ColumnData, value interface{}) (bool, error) {
	if r.current < 0 || r.current >= r.totalRows {
		return false, fmt.Errorf("invalid current index: %d", r.current)
	}

	colData, ok := col.(*ColumnData)
	if !ok {
		return false, fmt.Errorf("invalid column type: %T", col)
	}

	recordLength := colData.Length + 2
	offset := r.current * int64(recordLength)

	if r.current >= r.totalRows {
		return false, fmt.Errorf("invalid current index: %d", r.current)
	}

	if int(offset)+recordLength > len(colData.data) {
		return false, fmt.Errorf("offset out of bounds: %d", offset)
	}

	data := colData.data[offset : offset+int64(recordLength)]

	if data[0] != 1 {
		return false, nil
	}

	if err := colData.DataType.Read(data[1:], value); err != nil {
		return false, fmt.Errorf("failed to read value: %w", err)
	}

	return true, nil
}

func (r *ColumnReader) ReadValue(field string, value interface{}) error {
	col, ok := r.columnsData[field]
	if !ok {
		return fmt.Errorf("field %s not found", field)
	}

	_, err := r.FastGetValue(col, value)

	return err
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

	return col.DataType.Parse(data[1:]), nil
}

func (r *ColumnReader) Close() error {
	for _, col := range r.columnsData {
		col.FreeView(col.data)
	}

	return nil
}

func (r *ColumnReader) CurrentID() int64 {
	return r.current + 1
}

func (r *ColumnReader) ScanMap() map[string]*buffer.Scanner {
	result := make(map[string]*buffer.Scanner, len(r.columnsData))
	for name, col := range r.columnsData {
		c := col
		result[name] = &buffer.Scanner{
			Type: c.DataType,
			Scan: func(value interface{}) (bool, error) {
				return r.FastGetValue(c, value)
			},
		}
	}
	return result
}
