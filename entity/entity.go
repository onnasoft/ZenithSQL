package entity

import (
	"fmt"
	"strings"

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

func (e *Entity) SetByIndex(index int, value interface{}) error {
	if index < 0 || index >= len(e.values) {
		return fmt.Errorf("index %d out of range", index)
	}

	field, err := e.fields.Get(index)
	if err != nil {
		return fmt.Errorf("failed to get field %d: %v", index, err)
	}
	if !isValidType(field.Type, value) {
		log.Println("Value type mismatch for field:", field.Name, "expected:", field.Type, "got:", value)

		return fmt.Errorf("invalid type %T for field %s, expected %s", value, field.Name, field.Type)
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
		isSetted := writeSettedFlag(buffer, field.IsSettedFlagPos, val)
		if isSetted == 0 {
			log.Println("Field is not set:", field.Name)
			continue
		}

		if writerFunc, ok := writerTypes[field.Type]; ok {
			if err := writerFunc(buffer, field, val); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeSettedFlag(buffer []byte, isSettedFlagPos int, val interface{}) byte {
	var isSetted byte = 1
	if val == nil {
		isSetted = 0
	}
	buffer[isSettedFlagPos] = isSetted
	return isSetted
}

func (e *Entity) Read(buffer []byte) error {
	for _, field := range *e.fields {
		isSetted := buffer[field.IsSettedFlagPos]

		if isSetted == 0 {
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

		if index, ok := e.fieldsMap[field.Name]; ok {
			e.values[index] = value
		}
	}

	return nil
}
