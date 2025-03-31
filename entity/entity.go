package entity

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"

	"log"
)

type Entity struct {
	fieldsMap map[string]int
	fields    *Fields
	values    []interface{}
}

func NewEntity(fields *Fields) (*Entity, error) {
	fieldsMap := make(map[string]int)
	for i := 0; i < fields.Len(); i++ {
		field, err := fields.Get(i)
		if err != nil {
			return nil, fmt.Errorf("failed to get field %d: %v", i, err)
		}
		fieldsMap[field.Name] = i
	}

	return &Entity{
		fieldsMap: fieldsMap,
		fields:    fields,
		values:    make([]interface{}, fields.Len()),
	}, nil
}

func (e *Entity) Values() []interface{} {
	return e.values
}

func (e *Entity) Len() int {
	return len(e.values)
}

func (e Entity) Get(index interface{}) interface{} {
	switch index := index.(type) {
	case int:
		return e.GetByIndex(index)
	case string:
		return e.GetByName(index)
	}

	return nil
}

func (e Entity) GetByIndex(index int) interface{} {
	if index < 0 || index >= len(e.values) {
		return nil
	}
	return e.values[index]
}

func (e Entity) GetByName(name string) interface{} {
	if index, ok := e.fieldsMap[name]; ok {
		return e.GetByIndex(index)
	}
	return nil
}

func (e Entity) Set(index interface{}, value interface{}) error {
	switch index := index.(type) {
	case int:
		return e.SetByIndex(index, value)
	case string:
		return e.SetByName(index, value)
	}

	return nil
}

func (e *Entity) SetByIndex(index int, value interface{}, ignoreValidation ...bool) error {
	if index < 0 || index >= len(e.values) {
		return fmt.Errorf("index %d out of range", index)
	}

	field, err := e.fields.Get(index)
	if err != nil {
		return fmt.Errorf("failed to get field %d: %v", index, err)
	}
	if len(ignoreValidation) == 0 || !ignoreValidation[0] {
		if !isValidType(field.Type, value) {
			log.Println("Value type mismatch for field:", field.Name, "expected:", field.Type, "got:", value)

			return fmt.Errorf("invalid type %T for field %s, expected %s", value, field.Name, field.Type)
		}
	}
	for _, validator := range field.Validators {
		if err := validator.Validate(value, field.Name); err != nil {
			return fmt.Errorf("validation failed for field %s: %v", field.Name, err)
		}
	}

	e.values[index] = value

	return nil
}

func (e *Entity) SetByName(name string, value interface{}) error {
	if index, ok := e.fieldsMap[name]; ok {
		return e.SetByIndex(index, value)
	}

	return fmt.Errorf("field %s not found", name)
}

func (e Entity) String() string {
	var format = "%s: %v"
	var result = make([]string, 0, len(e.values))

	for i := 0; i < len(e.values); i++ {
		result = append(result, fmt.Sprintf(format, (*e.fields)[i].Name, e.Get(i)))
	}

	return fmt.Sprintf("{%s}", strings.Join(result, ", "))
}

func (e *Entity) Write(buffer []byte) error {
	for _, field := range *e.fields {
		val := e.GetByName(field.Name)

		// Write null flag
		isNull := writeNullFlag(buffer, field.NullFlagPos, val)
		if isNull == 0 {
			if err := writeValue(buffer, field, val); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeNullFlag(buffer []byte, nullFlagPos int, val interface{}) byte {
	var isNull byte
	if val == nil {
		isNull = 1
	}
	buffer[nullFlagPos] = isNull
	return isNull
}

func writeValue(buffer []byte, field *Field, val interface{}) error {
	switch field.Type {
	case Int8Type:
		buffer[field.StartPosition] = uint8(val.(int64))
	case Int16Type:
		binary.LittleEndian.PutUint16(buffer[field.StartPosition:], uint16(val.(int64)))
	case Int32Type:
		binary.LittleEndian.PutUint32(buffer[field.StartPosition:], uint32(val.(int64)))
	case Int64Type:
		binary.LittleEndian.PutUint64(buffer[field.StartPosition:], uint64(val.(int64)))
	case Uint8Type:
		buffer[field.StartPosition] = uint8(val.(int64))
	case Uint16Type:
		binary.LittleEndian.PutUint16(buffer[field.StartPosition:], uint16(val.(int64)))
	case Uint32Type:
		binary.LittleEndian.PutUint32(buffer[field.StartPosition:], uint32(val.(int64)))
	case Uint64Type:
		binary.LittleEndian.PutUint64(buffer[field.StartPosition:], uint64(val.(int64)))
	case Float32Type:
		binary.LittleEndian.PutUint32(buffer[field.StartPosition:], math.Float32bits(val.(float32)))
	case Float64Type:
		binary.LittleEndian.PutUint64(buffer[field.StartPosition:], math.Float64bits(val.(float64)))
	case StringType:
		str := val.(string)
		if len(str) > field.Length {
			return fmt.Errorf("string length exceeds maximum length of %d", field.Length)
		}
		copy(buffer[field.StartPosition:], str)
		for j := len(str); j < field.Length; j++ {
			buffer[field.StartPosition+j] = 0
		}
	case TimestampType:
		binary.LittleEndian.PutUint64(buffer[field.StartPosition:], uint64(val.(time.Time).UnixNano()))
	default:
		return fmt.Errorf("unsupported type %s for column %s", field.Type.String(), field.Name)
	}
	return nil
}

func (e *Entity) Read(buffer []byte) error {
	for _, field := range *e.fields {
		isNull := buffer[field.NullFlagPos]

		if isNull == 1 {
			continue
		}

		startPos := field.StartPosition
		endPos := field.EndPosition
		if endPos > len(buffer) {
			return fmt.Errorf("buffer length is less than expected for field %s", field.Name)
		}
		b := buffer[startPos:endPos]
		parser, ok := parseTypes[field.Type]
		if !ok {
			return fmt.Errorf("unsupported data type: %s", field.Type)
		}

		value := parser(b)
		if value == nil {
			return fmt.Errorf("failed to parse value for column %s", field.Name)
		}

		if err := e.SetByName(field.Name, value); err != nil {
			return fmt.Errorf("failed to set value for column %s: %v", field.Name, err)
		}
	}

	return nil
}
