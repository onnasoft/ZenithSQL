package entity

import (
	"fmt"
	"strings"
	"sync"

	"github.com/onnasoft/ZenithSQL/core/buffer"
)

type Entity struct {
	RW     buffer.ReadWriter
	mu     sync.RWMutex
	Schema *Schema
	values map[string]interface{}
	offset int
}

type EntityConfig struct {
	Schema *Schema
	RW     buffer.ReadWriter
}

func NewEntity(config *EntityConfig) (*Entity, error) {
	if config.RW == nil {
		return nil, fmt.Errorf("readwriter cannot be nil")
	}
	if config.Schema == nil {
		return nil, fmt.Errorf("schema cannot be nil")
	}

	return &Entity{
		Schema: config.Schema,
		RW:     config.RW,
		values: make(map[string]interface{}),
	}, nil
}

func (e *Entity) setValue(name string, value interface{}) {
	e.mu.Lock()
	e.values[name] = value
	e.mu.Unlock()
}

func (e *Entity) SetValue(name string, value interface{}) error {
	field, err := e.Schema.GetFieldByName(name)
	if err != nil {
		return err
	}
	if !isValidType(field.Type, value) {
		return fmt.Errorf("invalid type for field '%s': expected %s, got %T", field.Name, field.Type.String(), value)
	}

	for _, validator := range field.Validators {
		if err := validator.Validate(value, field.Name); err != nil {
			return err
		}
	}
	e.setValue(field.Name, value)
	return nil
}

func (e *Entity) GetValue(name string) interface{} {
	e.mu.RLock()
	val, ok := e.values[name]
	e.mu.RUnlock()
	if ok {
		return val
	}

	field, err := e.Schema.GetFieldByName(name)
	if err != nil {
		return nil
	}

	isSettedFlagByte := make([]byte, 1)
	e.RW.ReadAt(isSettedFlagByte, field.IsSettedFlagPos)
	if isSettedFlagByte[0] == 0 {
		e.mu.Lock()
		e.values[name] = nil
		e.mu.Unlock()
		return nil
	}

	data := make([]byte, field.Length)
	if _, err := e.RW.ReadAt(data, e.offset+field.StartPosition); err != nil {
		return nil
	}

	value, err := decodeField(field, data)
	if err == nil {
		e.mu.Lock()
		e.values[name] = value
		e.mu.Unlock()
	}

	return value
}

func (e *Entity) Save() error {
	row := make([]byte, e.Schema.Size())
	for _, field := range e.Schema.Fields {
		if err := encodeField(field, e.GetValue(field.Name), row[field.StartPosition:field.EndPosition]); err != nil {
			return err
		}
	}
	_, err := e.RW.Write(row)
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
	for i, field := range e.Schema.Fields {
		if i > 0 {
			sb.WriteString(", ")
		}
		val := e.GetValue(field.Name)
		sb.WriteString(fmt.Sprintf("%s: %v", field.Name, val))
	}
	sb.WriteString("}")
	return sb.String()
}
