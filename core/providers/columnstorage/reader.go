package columnstorage

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/core/storage"
)

type ColumnReader struct {
	columns map[string]*Column
	current int64
	stats   *storage.StorageStats
}

func NewColumnReader(columns map[string]*Column, stats *storage.StorageStats) storage.Reader {
	return &ColumnReader{
		columns: columns,
		current: -1,
		stats:   stats,
	}
}

func (r *ColumnReader) Next() bool {
	if r.current+1 >= r.stats.TotalRows {
		return false
	}
	r.current++
	return true
}

func (r *ColumnReader) Seek(id int64) error {
	fmt.Println("Seek", id)
	if id <= 0 || id > r.stats.TotalRows {
		return fmt.Errorf("invalid id: %d", id)
	}
	r.current = id - 1
	return nil
}

func (r *ColumnReader) Values() map[string]interface{} {
	values := make(map[string]interface{}, len(r.columns))
	for name, col := range r.columns {
		recordLength := col.Length + 2
		offset := r.current * int64(recordLength)
		fmt.Println("Column name", name)
		data := col.MMapFile.Data()[offset : offset+int64(recordLength)]

		if data[0] != 1 {
			values[name] = nil
			continue
		}

		values[name] = col.parser(data[1:])
	}
	return values
}

func (r *ColumnReader) ReadFieldValue(col *Column, value interface{}) {
	if r.current < 0 || r.current >= r.stats.TotalRows {
		return
	}
	recordLength := col.Length + 2
	offset := r.current * int64(recordLength)

	data := col.MMapFile.Data()[offset : offset+int64(recordLength)]

	if data[0] != 1 {
		return
	}

	col.read(data[1:], value)
}

func (r *ColumnReader) ReadValue(field string, value interface{}) {
	col, ok := r.columns[field]
	if !ok {
		return
	}

	recordLength := col.Length + 2
	offset := r.current * int64(recordLength)

	if r.current >= r.stats.TotalRows {
		return
	}

	data := col.MMapFile.Data()[offset : offset+int64(recordLength)]

	if data[0] != 1 {
		return
	}

	fmt.Println(data[0])
	fmt.Println(data[1:])

	col.read(data[1:], value)
}

func (r *ColumnReader) GetValue(field string) (interface{}, error) {
	col, ok := r.columns[field]
	if !ok {
		return nil, fmt.Errorf("field %s not found", field)
	}

	recordLength := col.Length + 2
	offset := r.current * int64(recordLength)
	data := col.MMapFile.Data()[offset : offset+int64(recordLength)]

	if data[0] != 1 {
		return nil, nil
	}

	return col.parser(data[1:]), nil
}

func (r *ColumnReader) Close() error {
	return nil
}
