package entity

import (
	"fmt"
	"strings"

	"github.com/onnasoft/ZenithSQL/core/buffer"
)

type StatelessEntity struct {
	rw     buffer.ReadWriter
	schema *Schema
	offset int
}

func newStatelessEntity(config *EntityConfig) (*StatelessEntity, error) {
	if config.RW == nil || config.Schema == nil {
		return nil, fmt.Errorf("invalid config")
	}
	return &StatelessEntity{
		schema: config.Schema,
		rw:     config.RW,
	}, nil
}

func (e *StatelessEntity) SetValue(name string, value interface{}) error {
	return fmt.Errorf("StatelessEntity does not support SetValue")
}

func (e *StatelessEntity) IsSetted() bool {
	isSet := make([]byte, 1)
	e.rw.ReadAt(isSet, e.schema.IsSettedFlagPos)
	return isSet[0] == 1
}

func (e *StatelessEntity) GetValue(name string) interface{} {
	field, _ := e.schema.GetFieldByName(name)
	isSet := make([]byte, 1)
	e.rw.ReadAt(isSet, e.schema.IsSettedFlagPos)
	if isSet[0] == 0 {
		return nil
	}
	data := make([]byte, field.Length)
	e.rw.ReadAt(data, e.offset+field.StartPosition)
	val, _ := decodeField(field, data)
	return val
}

func (e *StatelessEntity) Save() error {
	return fmt.Errorf("StatelessEntity does not support Save")
}

func (e *StatelessEntity) Reset() {}

func (e *StatelessEntity) Values() map[string]interface{} {
	result := make(map[string]interface{})
	for _, f := range e.schema.Fields {
		result[f.Name] = e.GetValue(f.Name)
	}
	return result
}

func (e *StatelessEntity) Len() int {
	return len(e.schema.Fields)
}

func (e *StatelessEntity) String() string {
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

func (e *StatelessEntity) RW() buffer.ReadWriter {
	return e.rw
}

func (e *StatelessEntity) Schema() *Schema {
	return e.schema
}
