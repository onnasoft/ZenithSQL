package dataframe

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/onnasoft/ZenithSQL/allocator"
	"github.com/onnasoft/ZenithSQL/validate"
)

type Table struct {
	Name           string
	Path           string
	Columns        *Columns
	length         int64
	reservedSize   int
	effectiveSize  int
	File           *os.File
	writeAllocator *allocator.ZeroMemoryAllocator // Allocator for writing, with fixed reserved size
	readAllocator  *allocator.ZeroMemoryAllocator // Allocator for reading, with dynamic size based on the row
}

func NewTable(name, path string) (*Table, error) {
	fullPath := filepath.Join(path, name)
	if err := os.MkdirAll(fullPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create table directory: %v", err)
	}

	// Open the file for storing data
	filePath := filepath.Join(fullPath, "data.bin")
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %v", err)
	}

	t := &Table{
		Name: name,
		Path: fullPath,
		Columns: &Columns{
			{Name: "id", Type: Int64Type, Length: 8},
			{Name: "created_at", Type: TimestampType, Length: 8},
			{Name: "updated_at", Type: TimestampType, Length: 8},
			{Name: "deleted_at", Type: TimestampType, Length: 8},
		},
		File: file,
	}
	t.reservedSize = 2048                        // Fixed reserved size for each row (2048 bytes)
	t.effectiveSize = t.calculateEffectiveSize() // Calculate the effective size based on the columns
	// Allocators for reading and writing
	t.writeAllocator = allocator.NewZeroMemoryAllocator(100, func() interface{} {
		return make([]byte, t.reservedSize)
	})

	t.readAllocator = allocator.NewZeroMemoryAllocator(100, func() interface{} {
		return make([]byte, t.effectiveSize)
	})

	t.setColumnPositions()

	return t, nil
}

func (t *Table) setColumnPositions() {
	offset := 0
	for i, col := range *t.Columns {
		col.StartPosition = offset
		col.EndPosition = offset + col.Length
		col.NullFlagPos = offset + len(*t.Columns)*1
		offset += col.Length + 1
		(*t.Columns)[i] = col
	}
}

func (t *Table) calculateEffectiveSize() int {
	size := 0
	for _, col := range *t.Columns {
		size += col.Length + 1 // +1 for null value
	}
	return size
}

func (t *Table) AddColumn(name string, typ DataType, length int, validators ...validate.Validator) error {
	if length <= 0 {
		return fmt.Errorf("invalid length for type %s", typ.String())
	}

	col := Column{
		Name:       name,
		Type:       typ,
		Length:     length,
		Validators: validators,
	}
	if typ == StringType && length > 0 {
		col.Validators = append(col.Validators, validate.StringLengthValidator{Min: 0, Max: length})
	}

	t.Columns.Add(col)
	t.reservedSize = 2048 // Fixed reserved size for each row (2048 bytes)

	t.setColumnPositions()

	t.effectiveSize = t.calculateEffectiveSize()
	t.readAllocator.Reset()
	t.readAllocator = allocator.NewZeroMemoryAllocator(100, func() interface{} {
		return make([]byte, t.effectiveSize)
	})

	return nil
}

func (t *Table) Insert(values ...interface{}) error {
	userColumns := t.Columns.Len() - 4
	if len(values) != userColumns {
		return fmt.Errorf("expected %d values, got %d", userColumns, len(values))
	}

	now := time.Now().UnixNano()
	row := NewRow(t.Columns)
	row.Set(0, int64(t.length+1))
	row.Set(1, now)
	row.Set(2, now)
	row.Set(3, nil)

	for i, val := range values {
		col := t.Columns.Get(i + 4)

		if !isValidType(col.Type, val) {
			return fmt.Errorf("column '%s' expects %s, got %T", col.Name, col.Type.String(), val)
		}

		for _, validator := range col.Validators {
			if err := validator.Validate(val, col.Name); err != nil {
				return err
			}
		}

		row.Set(i+4, val)
	}

	if err := t.writeRowToFile(row); err != nil {
		return err
	}

	return nil
}

func (t *Table) writeRowToFile(row *Row) error {
	// Allocate memory for writing
	buff, err := t.writeAllocator.Allocate()
	if err != nil {
		return fmt.Errorf("failed to allocate memory for row: %v", err)
	}
	defer t.writeAllocator.Release(buff)

	// Convert the buffer to a byte slice
	buffer := buff.([]byte)

	// Iterate over each column in the row and write the values
	err = row.Write(buffer)
	if err != nil {
		return fmt.Errorf("failed to write row to buffer: %v", err)
	}

	// Fill the remaining space with padding to ensure 2048 bytes
	for i := len(buffer); i < t.reservedSize; i++ {
		buffer[i] = 0
	}

	// Write the row to the file at the appropriate offset
	offset := (t.length) * int64(t.reservedSize)
	if _, err := t.File.WriteAt(buffer, offset); err != nil {
		return fmt.Errorf("failed to write row to file: %v", err)
	}

	// Flush the file to ensure the data is written
	if err := t.File.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %v", err)
	}

	// Increment row length
	t.length++

	return nil
}

func (t *Table) Close() {
	t.File.Close()
}

func (t *Table) readRowFromFile(id int64, columns *Columns) (*Row, error) {
	row := NewRow(columns)
	offset := (id - 1) * int64(t.reservedSize)

	// Allocate memory for reading with the effective size
	buff, err := t.readAllocator.Allocate()
	if err != nil {
		return nil, fmt.Errorf("failed to allocate memory for row %d: %v", id, err)
	}
	defer t.readAllocator.Release(buff)
	buffer := buff.([]byte)

	// Read the data into the buffer
	if _, err := t.File.ReadAt(buffer, offset); err != nil {
		return nil, fmt.Errorf("failed to read row %d: %v", id, err)
	}

	// Parse the row data from the buffer
	err = row.Read(buffer)
	if err != nil {
		return nil, fmt.Errorf("failed to parse row %d: %v", id, err)
	}

	return row, nil
}

func (t *Table) Get(id int64, columns *Columns) (*Row, error) {
	if id < 0 || id > t.length {
		return nil, fmt.Errorf("invalid row index %d", id)
	}

	row, err := t.readRowFromFile(id, columns)
	if err != nil {
		return nil, fmt.Errorf("failed to read row %d: %v", id, err)
	}

	return row, nil
}

func (t *Table) Length() int64 {
	return t.length
}

func (t *Table) EffectiveSize() int {
	return t.effectiveSize
}
func (t *Table) ReservedSize() int {
	return t.reservedSize
}

func (t *Table) Print() {
	fmt.Println("Table Name:", t.Name)
	fmt.Println("Table Columns:")
	format := " - %s (%s)\n"
	fmt.Printf(format, t.Columns.Get(0).Name, t.Columns.Get(0).Type.String())

	for i := 4; i < t.Columns.Len(); i++ {
		fmt.Printf(format, t.Columns.Get(i).Name, t.Columns.Get(i).Type.String())
	}
	fmt.Printf(format, t.Columns.Get(1).Name, t.Columns.Get(1).Type.String())
	fmt.Printf(format, t.Columns.Get(2).Name, t.Columns.Get(2).Type.String())
	fmt.Printf(format, t.Columns.Get(3).Name, t.Columns.Get(3).Type.String())

	fmt.Println("Table Length:", t.length)
	fmt.Println("Table Reserved Size:", t.reservedSize)
	fmt.Println("Table Effective Size:", t.effectiveSize)
	fmt.Println("Table Path:", t.Path)
	fmt.Println("Table File:", t.File.Name())
}
