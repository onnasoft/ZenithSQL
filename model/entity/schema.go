package entity

import (
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/onnasoft/ZenithSQL/validate"
)

const (
	errIndexOutOfRange = "index out of range"
)

func (dt DataType) IsNumeric() bool {
	return dt == Int64Type || dt == Float64Type
}

type Schema struct {
	fields    []*Field
	nameIndex map[string]int
	mu        sync.RWMutex
}

func NewSchema() *Schema {
	return &Schema{
		fields:    make([]*Field, 0),
		nameIndex: make(map[string]int),
	}
}

func (f *Schema) Iter() []*Field {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.fields
}

func (f *Schema) Len() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.fields)
}

func (f *Schema) Get(index int) (*Field, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if index < 0 || index >= len(f.fields) {
		return nil, errors.New(errIndexOutOfRange)
	}
	return f.fields[index], nil
}

func (fs *Schema) CalculateSize() int {
	total := 0
	for _, f := range fs.fields {
		total += 1 + f.Length // 1 byte para el flag + longitud del campo
	}
	return total
}

func (fs *Schema) PrepareOffsets() {
	offset := 0
	log.Println("Preparing offsets for fields...", fs.Len())
	for _, f := range fs.Iter() {
		log.Println("Preparing field:", f.Name)
		if f.Length <= 0 {
			panic(fmt.Sprintf("invalid length %d for field %s", f.Length, f.Name))
		}
		offset += 1 // 1 byte para el flag
		//f.Prepare(offset)
		//offset += 1 + f.Length // Avanzar el offset
	}
}

func (f *Schema) GetByName(name string) (*Field, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if index, ok := f.IndexOf(name); ok {
		return f.fields[index], nil
	}
	return nil, fmt.Errorf("field %s not found", name)
}

func (f *Schema) Insert(index int, field *Field) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if index < 0 || index > len(f.fields) {
		return errors.New(errIndexOutOfRange)
	}

	// Check for duplicate name
	if _, exists := f.nameIndex[field.Name]; exists {
		return fmt.Errorf("field %s already exists", field.Name)
	}

	// Insert field
	f.fields = append(f.fields, nil)
	copy(f.fields[index+1:], f.fields[index:])
	f.fields[index] = field

	// Update name index
	f.rebuildIndex()

	return nil
}

func (f *Schema) Add(field *Field) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, exists := f.nameIndex[field.Name]; exists {
		return fmt.Errorf("field %s already exists", field.Name)
	}

	f.fields = append(f.fields, field)
	f.nameIndex[field.Name] = len(f.fields) - 1
	return nil
}

func (f *Schema) Remove(index int) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if index < 0 || index >= len(f.fields) {
		return errors.New(errIndexOutOfRange)
	}

	// Remove from name index first
	delete(f.nameIndex, f.fields[index].Name)

	// Remove from fields slice
	copy(f.fields[index:], f.fields[index+1:])
	f.fields = f.fields[:len(f.fields)-1]

	// Rebuild index for remaining fields
	f.rebuildIndex()

	return nil
}

func (f *Schema) RemoveByName(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	index, ok := f.nameIndex[name]
	if !ok {
		return fmt.Errorf("field %s not found", name)
	}

	// Remove from fields slice
	copy(f.fields[index:], f.fields[index+1:])
	f.fields = f.fields[:len(f.fields)-1]

	// Rebuild entire index
	f.rebuildIndex()

	return nil
}

func (f *Schema) IndexOf(name string) (int, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	index, ok := f.nameIndex[name]
	return index, ok
}

func (f *Schema) rebuildIndex() {
	f.nameIndex = make(map[string]int)
	for i, field := range f.fields {
		f.nameIndex[field.Name] = i
	}
}

func (f *Schema) Clone() *Schema {
	f.mu.RLock()
	defer f.mu.RUnlock()

	newSchema := NewSchema()
	for _, field := range f.fields {
		// Create a deep copy of the field
		newField := &Field{
			Name:       field.Name,
			Type:       field.Type,
			Length:     field.Length,
			Validators: make([]validate.Validator, len(field.Validators)),
		}
		copy(newField.Validators, field.Validators)
		newSchema.Add(newField)
	}
	newSchema.PrepareOffsets()
	return newSchema
}
