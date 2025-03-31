package dataframe

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/onnasoft/ZenithSQL/allocator"
	"github.com/onnasoft/ZenithSQL/entity"
	"github.com/onnasoft/ZenithSQL/validate"
)

type Table struct {
	Name           string
	Path           string
	Fields         *entity.Fields
	length         int64
	reservedSize   int
	effectiveSize  int
	File           *os.File
	writeAllocator *allocator.ZeroMemoryAllocator
	readAllocator  *allocator.ZeroMemoryAllocator
}

func NewTable(name, path string) (*Table, error) {
	fullPath := filepath.Join(path, name)
	if err := os.MkdirAll(fullPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create table directory: %v", err)
	}

	filePath := filepath.Join(fullPath, "data.bin")
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %v", err)
	}

	t := &Table{
		Name: name,
		Path: fullPath,
		Fields: &entity.Fields{
			{Name: "id", Type: entity.Int64Type, Length: 8},
			{Name: "created_at", Type: entity.TimestampType, Length: 8},
			{Name: "updated_at", Type: entity.TimestampType, Length: 8},
			{Name: "deleted_at", Type: entity.TimestampType, Length: 8},
		},
		File: file,
	}
	t.reservedSize = 2048
	t.effectiveSize = t.calculateEffectiveSize()

	t.writeAllocator = allocator.NewZeroMemoryAllocator(100, func() interface{} {
		return make([]byte, t.reservedSize)
	})

	t.readAllocator = allocator.NewZeroMemoryAllocator(100, func() interface{} {
		return make([]byte, t.effectiveSize)
	})

	t.setColumnPositions()

	return t, nil
}

func (t *Table) GetNextId() int64 {
	return t.length + 1
}

func (t *Table) setColumnPositions() {
	offset := 0
	for _, col := range *t.Fields {
		col.Prepare(offset)
		offset += col.Length + 1
	}
}

func (t *Table) calculateEffectiveSize() int {
	size := 0
	for _, col := range *t.Fields {
		size += col.Length + 1
	}
	return size
}

func (t *Table) AddColumn(name string, typ entity.DataType, length int, validators ...validate.Validator) error {
	if length <= 0 {
		return fmt.Errorf("invalid length for type %s", typ.String())
	}

	col := &entity.Field{
		Name:       name,
		Type:       typ,
		Length:     length,
		Validators: validators,
	}
	if typ == entity.StringType && length > 0 {
		col.Validators = append(col.Validators, validate.StringLengthValidator{Min: 0, Max: length})
	}

	t.Fields.Add(col)
	t.reservedSize = 2048

	col.Prepare(t.effectiveSize)
	t.effectiveSize += col.Length + 1

	t.readAllocator.Reset()
	t.readAllocator = allocator.NewZeroMemoryAllocator(100, func() interface{} {
		return make([]byte, t.effectiveSize)
	})

	return nil
}

func (t *Table) Insert(entities ...*entity.Entity) error {
	id := t.GetNextId()
	for _, entity := range entities {
		if entity.GetByName("created_at") == nil {
			entity.SetByName("created_at", time.Now())
		}
		if entity.GetByName("updated_at") == nil {
			entity.SetByName("updated_at", time.Now())
		}

		userfields := t.Fields.Len()
		if entity.Len() != userfields {
			return fmt.Errorf("expected %d values, got %d", userfields, entity.Values())
		}
		entity.SetByName("id", id)
		if err := t.writeRowToFile(entity); err != nil {
			return err
		}

		id++
	}

	return nil
}

func (t *Table) writeRowToFile(entity *entity.Entity) error {
	buff, err := t.writeAllocator.Allocate()
	if err != nil {
		return fmt.Errorf("failed to allocate memory for row: %v", err)
	}
	defer t.writeAllocator.Release(buff)

	buffer := buff.([]byte)

	err = entity.Write(buffer)
	if err != nil {
		return fmt.Errorf("failed to write row to buffer: %v", err)
	}

	for i := len(buffer); i < t.reservedSize; i++ {
		buffer[i] = 0
	}

	offset := (t.length) * int64(t.reservedSize)
	if _, err := t.File.WriteAt(buffer, offset); err != nil {
		return fmt.Errorf("failed to write row to file: %v", err)
	}

	if err := t.File.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %v", err)
	}

	t.length++

	return nil
}

func (t *Table) Close() {
	t.File.Close()
}

func (t *Table) readRowFromFile(id int64, row *entity.Entity) error {
	offset := (id - 1) * int64(t.reservedSize)

	buff, err := t.readAllocator.Allocate()
	if err != nil {
		return fmt.Errorf("failed to allocate memory for row %d: %v", id, err)
	}
	defer t.readAllocator.Release(buff)
	buffer := buff.([]byte)

	if _, err := t.File.ReadAt(buffer, offset); err != nil {
		return fmt.Errorf("failed to read row %d: %v", id, err)
	}

	err = row.Read(buffer)
	if err != nil {
		return fmt.Errorf("failed to parse row %d: %v", id, err)
	}

	return nil
}

func (t *Table) Get(id int64, row *entity.Entity) error {
	if id < 0 {
		return fmt.Errorf("invalid row index %d", id)
	}

	err := t.readRowFromFile(id, row)
	if err != nil {
		return fmt.Errorf("failed to read row %d: %v", id, err)
	}

	return nil
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
	fmt.Println("Table fields:")
	format := " - %s (%s)\n"
	fields := *t.Fields

	if fields.Len() < 4 {
		fmt.Printf(format, fields[0].Name, fields[0].Type.String())

		for i := 4; i < t.Fields.Len(); i++ {
			fmt.Printf(format, fields[i].Name, fields[i].Type.String())
		}
		fmt.Printf(format, fields[1].Name, fields[1].Type.String())
		fmt.Printf(format, fields[2].Name, fields[2].Type.String())
		fmt.Printf(format, fields[3].Name, fields[3].Type.String())
	} else {
		for i := 0; i < fields.Len(); i++ {
			fmt.Printf(format, fields[i].Name, fields[i].Type.String())
		}
	}

	fmt.Println("Table Length:", t.length)
	fmt.Println("Table Reserved Size:", t.reservedSize)
	fmt.Println("Table Effective Size:", t.effectiveSize)
	fmt.Println("Table Path:", t.Path)
	fmt.Println("Table File:", t.File.Name())
}
