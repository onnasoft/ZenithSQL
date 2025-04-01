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

type Field struct {
	Name            string
	Type            DataType
	Length          int
	Validators      []validate.Validator
	StartPosition   int
	EndPosition     int
	IsSettedFlagPos int
}

type Fields struct {
	fields    []*Field
	nameIndex map[string]int
	mu        sync.RWMutex
}

func NewFields() *Fields {
	return &Fields{
		fields:    make([]*Field, 0),
		nameIndex: make(map[string]int),
	}
}
func (e *Field) IsNumeric() bool {
	return e.Type == Int64Type || e.Type == Float64Type
}

func (e *Field) IsString() bool {
	return e.Type == StringType
}
func (e *Field) IsBool() bool {
	return e.Type == BoolType
}

func (e *Field) IsDate() bool {
	return e.Type == TimestampType
}

func (e *Field) DecodeNumeric(buffer []byte) (interface{}, error) {
	if parseFunc, ok := parseTypes[e.Type]; ok {
		return parseFunc(buffer), nil
	}
	return nil, fmt.Errorf("unsupported type %s", e.Type)
}

func (f *Fields) Iter() []*Field {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.fields
}

func (f *Fields) Len() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.fields)
}

func (f *Field) Prepare(offset int) {
	if f.Length <= 0 {
		log.Fatalf("invalid length %d for field %s", f.Length, f.Name)
	}

	f.IsSettedFlagPos = offset
	f.StartPosition = offset + 1 // El byte después del flag
	f.EndPosition = f.StartPosition + f.Length
}

func (f *Fields) Get(index int) (*Field, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if index < 0 || index >= len(f.fields) {
		return nil, errors.New(errIndexOutOfRange)
	}
	return f.fields[index], nil
}

func (fs *Fields) CalculateSize() int {
	total := 0
	for _, f := range fs.fields {
		total += 1 + f.Length // 1 byte para el flag + longitud del campo
	}
	return total
}

func (fs *Fields) PrepareOffsets() {
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

func (f *Fields) GetByName(name string) (*Field, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if index, ok := f.nameIndex[name]; ok {
		return f.fields[index], nil
	}
	return nil, fmt.Errorf("field %s not found", name)
}

func (f *Fields) Insert(index int, field *Field) error {
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

func (f *Fields) Add(field *Field) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, exists := f.nameIndex[field.Name]; exists {
		return fmt.Errorf("field %s already exists", field.Name)
	}

	f.fields = append(f.fields, field)
	f.nameIndex[field.Name] = len(f.fields) - 1
	return nil
}

func (f *Fields) Remove(index int) error {
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

func (f *Fields) RemoveByName(name string) error {
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

func (f *Fields) IndexOf(name string) (int, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	index, ok := f.nameIndex[name]
	return index, ok
}

func (f *Fields) rebuildIndex() {
	f.nameIndex = make(map[string]int)
	for i, field := range f.fields {
		f.nameIndex[field.Name] = i
	}
}

func (f *Fields) Clone() *Fields {
	f.mu.RLock()
	defer f.mu.RUnlock()

	newFields := NewFields()
	for _, field := range f.fields {
		// Create a deep copy of the field
		newField := &Field{
			Name:       field.Name,
			Type:       field.Type,
			Length:     field.Length,
			Validators: make([]validate.Validator, len(field.Validators)),
		}
		copy(newField.Validators, field.Validators)
		newFields.Add(newField)
	}
	newFields.PrepareOffsets()
	return newFields
}
