package entity

import (
	"errors"
	"fmt"
	"sync"
)

const (
	errIndexOutOfRange = "index out of range"
)

func (dt DataType) IsNumeric() bool {
	return dt == Int64Type || dt == Float64Type
}

type Schema struct {
	Fields    []*Field
	nameIndex map[string]int
	mu        sync.RWMutex
	size      int
	lock      bool
}

func NewSchema() *Schema {
	return &Schema{
		size:      1,
		Fields:    make([]*Field, 0),
		nameIndex: make(map[string]int),
	}
}

func (f *Schema) Iter() []*Field {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.Fields
}

func (f *Schema) Len() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.Fields)
}

func (fs *Schema) CalculateSize() int {
	total := 1
	for _, f := range fs.Fields {
		total += 1 + f.Length
	}
	return total
}

func (f *Schema) Size() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.size
}

func (f *Schema) GetField(index int) (*Field, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if index < 0 || index >= len(f.Fields) {
		return nil, errors.New(errIndexOutOfRange)
	}
	return f.Fields[index], nil
}

func (f *Schema) GetFieldByName(name string) (*Field, error) {
	if index, ok := f.IndexOf(name); ok {
		return f.Fields[index], nil
	}
	return nil, fmt.Errorf("field %s not found", name)
}

func (f *Schema) AddField(field *Field) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.lock {
		return fmt.Errorf("schema is locked")
	}

	if _, exists := f.nameIndex[field.Name]; exists {
		return fmt.Errorf("field %s already exists", field.Name)
	}

	field.Prepare(f.size)
	f.size += field.Length + 1
	f.Fields = append(f.Fields, field)
	f.nameIndex[field.Name] = len(f.Fields) - 1
	return nil
}

func (f *Schema) IndexOf(name string) (int, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	index, ok := f.nameIndex[name]
	return index, ok
}

func (f *Schema) Lock() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lock = true
}

func (f *Schema) IsLocked() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.lock
}
