package entity

import (
	"fmt"
	"strings"
	"sync"
)

type Entity struct {
	mu            sync.RWMutex
	checkValues   bool
	fields        *Fields
	values        []interface{}
	selectiveMode bool
	selected      map[string]struct{}
}

func NewEntity(fields *Fields) (*Entity, error) {
	if fields == nil {
		return nil, fmt.Errorf("fields cannot be nil")
	}

	return &Entity{
		checkValues: true,
		fields:      fields,
		values:      make([]interface{}, fields.Len()),
		selected:    make(map[string]struct{}),
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

// PrepareSelective configura la entidad para lectura/escritura selectiva
func (e *Entity) PrepareSelective(fields ...string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.selectiveMode = len(fields) > 0
	e.selected = make(map[string]struct{})
	for _, f := range fields {
		e.selected[f] = struct{}{}
	}
}

// SetFieldDirect establece un campo directamente desde bytes serializados
func (e *Entity) SetFieldDirect(name string, data []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	field, err := e.fields.GetByName(name)
	if err != nil {
		return err
	}

	value, err := decodeField(field, data)
	if err != nil {
		return err
	}

	index, _ := e.fields.IndexOf(name)
	e.values[index] = value
	return nil
}

// Read lee datos desde un buffer
func (e *Entity) Read(buffer []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.selectiveMode {
		return e.readSelective(buffer)
	}
	return e.readFull(buffer)
}

func (e *Entity) readFull(buffer []byte) error {
	for i := 0; i < e.fields.Len(); i++ {
		field, _ := e.fields.Get(i)
		if err := e.readField(field, buffer); err != nil {
			return err
		}
	}
	return nil
}

func (e *Entity) readSelective(buffer []byte) error {
	for name := range e.selected {
		field, err := e.fields.GetByName(name)
		if err != nil {
			continue // Saltar campos no encontrados
		}
		if err := e.readField(field, buffer); err != nil {
			return err
		}
	}
	return nil
}

func (e *Entity) readField(field *Field, buffer []byte) error {
	if field.IsSettedFlagPos >= len(buffer) {
		return fmt.Errorf("buffer too small for field %s", field.Name)
	}

	if buffer[field.IsSettedFlagPos] == 0 {
		index, _ := e.fields.IndexOf(field.Name)
		e.values[index] = nil
		return nil
	}

	if field.EndPosition > len(buffer) {
		return fmt.Errorf("buffer too small for field %s data", field.Name)
	}

	value, err := decodeField(field, buffer[field.StartPosition:field.EndPosition])
	if err != nil {
		return err
	}

	index, _ := e.fields.IndexOf(field.Name)
	e.values[index] = value
	return nil
}

func (e *Entity) writeFull(buffer []byte) error {
	for i := 0; i < e.fields.Len(); i++ {
		field, _ := e.fields.Get(i)
		if err := e.writeField(field, buffer); err != nil {
			return err
		}
	}
	return nil
}

func (e *Entity) writeSelective(buffer []byte) error {
	for name := range e.selected {
		field, err := e.fields.GetByName(name)
		if err != nil {
			continue // Saltar campos no encontrados
		}
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
	minSize := e.fields.CalculateSize()
	if len(buffer) < minSize {
		return fmt.Errorf("buffer too small for entity (required: %d, got: %d)", minSize, len(buffer))
	}

	if e.selectiveMode {
		return e.writeSelective(buffer)
	}
	return e.writeFull(buffer)
}

func (e *Entity) writeField(field *Field, buffer []byte) error {
	// Verificación exhaustiva de límites
	if field.EndPosition > len(buffer) {
		return fmt.Errorf("buffer overflow for field %s (required to %d, buffer size: %d)",
			field.Name, field.EndPosition, len(buffer))
	}

	index, _ := e.fields.IndexOf(field.Name)
	value := e.values[index]

	// Escribir flag de valor establecido
	if value == nil {
		buffer[field.IsSettedFlagPos] = 0
		return nil
	}
	buffer[field.IsSettedFlagPos] = 1

	// Validación de tipo
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

// clear llena un slice con ceros
func clear(buf []byte) {
	for i := range buf {
		buf[i] = 0
	}
}

// Reset limpia la entidad para reutilización
func (e *Entity) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()

	for i := range e.values {
		e.values[i] = nil
	}
	e.selectiveMode = false
	e.selected = make(map[string]struct{})
}

// Métodos de acceso básicos
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

	index, ok := e.fields.IndexOf(name)
	if !ok {
		return nil
	}
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

	field, err := e.fields.Get(index)
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

	index, ok := e.fields.IndexOf(name)
	if !ok {
		return fmt.Errorf("field not found: %s", name)
	}

	field, err := e.fields.Get(index)
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
	for i := 0; i < e.fields.Len(); i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		field, _ := e.fields.Get(i)
		sb.WriteString(fmt.Sprintf("%s: %v", field.Name, e.values[i]))
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
