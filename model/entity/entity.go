package entity

import (
	"fmt"
	"strings"
	"sync"

	"github.com/onnasoft/ZenithSQL/core/buffer"
)

type EntityConfig struct {
	Schema *Schema
	RW     buffer.ReadWriter
}

type Entity struct {
	rw     buffer.ReadWriter
	mu     sync.RWMutex
	schema *Schema
	values map[string]interface{}
}

func NewEntity(config *EntityConfig) (*Entity, error) {
	if config.RW == nil || config.Schema == nil {
		return nil, fmt.Errorf("invalid config")
	}
	return &Entity{
		schema: config.Schema,
		rw:     config.RW,
		values: make(map[string]interface{}),
	}, nil
}

func (e *Entity) SetValue(name string, value interface{}) error {
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

func (e *Entity) GetValue(name string) interface{} {
	e.mu.RLock()
	val, ok := e.values[name]
	e.mu.RUnlock()
	if ok {
		return val
	}

	field, _ := e.schema.GetFieldByName(name)
	value := GetValue(field, e.rw)

	e.mu.Lock()
	e.values[name] = value
	e.mu.Unlock()
	return value
}

func (e *Entity) IsSetted() bool {
	isSet := make([]byte, 1)
	e.rw.ReadAt(isSet, e.schema.IsSettedFlagPos)
	return isSet[0] == 1
}

func (e *Entity) Save() error {
	row := make([]byte, e.schema.Size())
	row[e.schema.IsSettedFlagPos] = 1
	for _, field := range e.schema.Fields {
		row[field.IsSettedFlagPos] = 1
		encodeField(field, e.GetValue(field.Name), row[field.StartPosition:field.EndPosition])
	}
	_, err := e.rw.Write(row)
	return err
}

func (e *Entity) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.values = make(map[string]interface{})
}

func (e *Entity) Values() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()
	copy := make(map[string]interface{}, len(e.values))
	for k, v := range e.values {
		copy[k] = v
	}
	return copy
}

func (e *Entity) Len() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.values)
}

func (e *Entity) String() string {
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

func (e *Entity) RW() buffer.ReadWriter {
	return e.rw
}

func (e *Entity) Schema() *Schema {
	return e.schema
}
