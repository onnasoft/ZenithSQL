package entity

import (
	"fmt"
	"strings"
	"sync"
)

type Entity struct {
	buff        []byte
	mu          sync.RWMutex
	checkValues bool
	Schema      *Schema
	values      []interface{}
}

func NewEntity(fields *Schema) (*Entity, error) {
	if fields == nil {
		return nil, fmt.Errorf("fields cannot be nil")
	}

	return &Entity{
		checkValues: true,
		Schema:      fields,
		values:      make([]interface{}, fields.Len()),
	}, nil
}

// EnableValidation activa la validación de valores
func (e *Entity) EnableValidation() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.checkValues = true
}

func (e *Entity) DisableValidation() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.checkValues = false
}

func (e *Entity) IsValidationEnabled() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.checkValues
}

// SetFieldDirect establece un campo directamente desde bytes serializados
func (e *Entity) SetFieldDirect(name string, data []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	field, err := e.Schema.GetByName(name)
	if err != nil {
		return err
	}

	value, err := decodeField(field, data)
	if err != nil {
		return err
	}

	index, _ := e.Schema.IndexOf(name)
	e.values[index] = value
	return nil
}

func (e *Entity) Read(buffer []byte) error {
	e.buff = buffer
	return nil
}

func (e *Entity) readField(field *Field) error {
	index, _ := e.Schema.IndexOf(field.Name)
	if e.values[index] != nil {
		return nil
	}

	if field.IsSettedFlagPos >= len(e.buff) {
		return fmt.Errorf("buffer too small for field %s", field.Name)
	}

	if e.buff[field.IsSettedFlagPos] == 0 {
		e.values[index] = nil
		return nil
	}

	if field.EndPosition > len(e.buff) {
		return fmt.Errorf("buffer too small for field %s data", field.Name)
	}

	value, err := decodeField(field, e.buff[field.StartPosition:field.EndPosition])
	if err != nil {
		return err
	}

	e.values[index] = value
	return nil
}

func (e *Entity) writeFull(buffer []byte) error {
	for i := 0; i < e.Schema.Len(); i++ {
		field, _ := e.Schema.Get(i)
		if err := e.writeField(field, buffer); err != nil {
			return err
		}
	}
	return nil
}

func (e *Entity) Write(buffer []byte) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Verificar tamaño mínimo del buffer
	minSize := e.Schema.CalculateSize()
	if len(buffer) < minSize {
		return fmt.Errorf("buffer too small for entity (required: %d, got: %d)", minSize, len(buffer))
	}

	return e.writeFull(buffer)
}

func (e *Entity) writeField(field *Field, buffer []byte) error {
	if field.EndPosition > len(buffer) {
		return fmt.Errorf("buffer overflow for field %s (required to %d, buffer size: %d)",
			field.Name, field.EndPosition, len(buffer))
	}

	index, _ := e.Schema.IndexOf(field.Name)
	value := e.values[index]

	if value == nil {
		buffer[field.IsSettedFlagPos] = 0
		return nil
	}
	buffer[field.IsSettedFlagPos] = 1

	if e.checkValues && !isValidType(field.Type, value) {
		return fmt.Errorf("invalid type %T for field %s (expected %s)",
			value, field.Name, field.Type)
	}

	dataSegment := buffer[field.StartPosition:field.EndPosition:field.EndPosition]
	return encodeField(field, value, dataSegment)
}

func encodeField(field *Field, value interface{}, buffer []byte) error {
	if field.Length <= 0 {
		return fmt.Errorf("invalid field length %d for %s", field.Length, field.Name)
	}
	if len(buffer) < field.Length {
		return fmt.Errorf("buffer too small for field %s (need %d, got %d, start: %d, end: %d)",
			field.Name, field.Length, len(buffer), field.StartPosition, field.EndPosition)
	}
	if value == nil {
		return nil
	}

	if writer, ok := writerTypes[field.Type]; ok {
		return writer(buffer, field, value)
	}
	return fmt.Errorf("unsupported field type: %s", field.Type)
}

func clear(buf []byte) {
	for i := range buf {
		buf[i] = 0
	}
}

func (e *Entity) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()

	for i := range e.values {
		e.values[i] = nil
	}
}

func (e *Entity) Values() []interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return append([]interface{}{}, e.values...)
}

func (e *Entity) Len() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.values)
}

func (e *Entity) Get(index interface{}) interface{} {
	switch v := index.(type) {
	case int:
		return e.GetByIndex(v)
	case string:
		return e.GetByName(v)
	default:
		return nil
	}
}

func (e *Entity) GetByIndex(index int) interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if index < 0 || index >= len(e.values) {
		return nil
	}
	return e.values[index]
}

func (e *Entity) GetByName(name string) interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()

	field, err := e.Schema.GetByName(name)
	if err != nil {
		return nil
	}
	e.readField(field)
	index, _ := e.Schema.IndexOf(field.Name)
	return e.values[index]
}

func (e *Entity) Set(index interface{}, value interface{}) error {
	switch v := index.(type) {
	case int:
		return e.SetByIndex(v, value)
	case string:
		return e.SetByName(v, value)
	default:
		return fmt.Errorf("invalid index type: %T", index)
	}
}

func (e *Entity) SetByIndex(index int, value interface{}) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if index < 0 || index >= len(e.values) {
		return fmt.Errorf("index out of range: %d", index)
	}

	field, err := e.Schema.Get(index)
	if err != nil {
		return err
	}

	if e.checkValues {
		if !isValidType(field.Type, value) {
			return fmt.Errorf("invalid type for field %s", field.Name)
		}
		for _, validator := range field.Validators {
			if err := validator.Validate(value, field.Name); err != nil {
				return err
			}
		}
	}

	e.values[index] = value
	return nil
}

func (e *Entity) SetByName(name string, value interface{}) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	index, ok := e.Schema.IndexOf(name)
	if !ok {
		return fmt.Errorf("field not found: %s", name)
	}

	field, err := e.Schema.Get(index)
	if err != nil {
		return err
	}

	if e.checkValues {
		if !isValidType(field.Type, value) {
			return fmt.Errorf("invalid type for field %s", field.Name)
		}
		for _, validator := range field.Validators {
			if err := validator.Validate(value, field.Name); err != nil {
				return err
			}
		}
	}

	e.values[index] = value
	return nil
}

func (e *Entity) String() string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var sb strings.Builder
	sb.WriteString("{")
	for i := 0; i < e.Schema.Len(); i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		field, _ := e.Schema.Get(i)
		sb.WriteString(fmt.Sprintf("%s: %v", field.Name, e.GetByName(field.Name)))
	}
	sb.WriteString("}")
	return sb.String()
}

func decodeField(field *Field, data []byte) (interface{}, error) {
	if parser, ok := parseTypes[field.Type]; ok {
		return parser(data), nil
	}
	return nil, fmt.Errorf("unsupported field type: %s", field.Type)
}
