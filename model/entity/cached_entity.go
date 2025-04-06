package entity

import (
	"fmt"
	"strings"
	"sync"

	"github.com/onnasoft/ZenithSQL/core/buffer"
)

type CachedEntity struct {
	rw     buffer.ReadWriter
	mu     sync.RWMutex
	schema *Schema
	values map[string]interface{}
	offset int
}

func newCachedEntity(config *EntityConfig) (*CachedEntity, error) {
	if config.RW == nil || config.Schema == nil {
		return nil, fmt.Errorf("invalid config")
	}
	return &CachedEntity{
		schema: config.Schema,
		rw:     config.RW,
		values: make(map[string]interface{}),
	}, nil
}

func (e *CachedEntity) SetValue(name string, value interface{}) error {
	field, err := e.schema.GetFieldByName(name)
	if err != nil {
		return err
	}
	if !isValidType(field.Type, value) {
		return fmt.Errorf("invalid type for field '%s'", field.Name)
	}
	for _, validator := range field.Validators {
		if err := validator.Validate(value, field.Name); err != nil {
			return err
		}
	}
	e.mu.Lock()
	e.values[name] = value
	e.mu.Unlock()
	return nil
}

func (e *CachedEntity) GetValue(name string) interface{} {
	e.mu.RLock()
	val, ok := e.values[name]
	e.mu.RUnlock()
	if ok {
		return val
	}

	field, _ := e.schema.GetFieldByName(name)

	isSet := make([]byte, 1)
	e.rw.ReadAt(isSet, e.schema.IsSettedFlagPos)
	if isSet[0] == 0 {
		e.mu.Lock()
		e.values[name] = nil
		e.mu.Unlock()
		return nil
	}

	data := make([]byte, field.Length)
	e.rw.ReadAt(data, e.offset+field.StartPosition)
	value, _ := decodeField(field, data)

	e.mu.Lock()
	e.values[name] = value
	e.mu.Unlock()
	return value
}

func (e *CachedEntity) IsSetted() bool {
	isSet := make([]byte, 1)
	e.rw.ReadAt(isSet, e.schema.IsSettedFlagPos)
	return isSet[0] == 1
}

func (e *CachedEntity) Save() error {
	row := make([]byte, e.schema.Size())
	row[e.schema.IsSettedFlagPos] = 1
	for _, field := range e.schema.Fields {
		encodeField(field, e.GetValue(field.Name), row[field.StartPosition:field.EndPosition])
	}
	_, err := e.rw.Write(row)
	return err
}

func (e *CachedEntity) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.values = make(map[string]interface{})
}

func (e *CachedEntity) Values() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()
	copy := make(map[string]interface{}, len(e.values))
	for k, v := range e.values {
		copy[k] = v
	}
	return copy
}

func (e *CachedEntity) Len() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.values)
}

func (e *CachedEntity) String() string {
	var sb strings.Builder
	sb.WriteString("{")
	for i, f := range e.schema.Fields {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("%s: %v", f.Name, e.GetValue(f.Name)))
	}
	sb.WriteString("}")
	return sb.String()
}

func (e *CachedEntity) RW() buffer.ReadWriter {
	return e.rw
}

func (e *CachedEntity) Schema() *Schema {
	return e.schema
}
