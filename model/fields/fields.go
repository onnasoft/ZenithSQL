package fields

import (
	"encoding/json"
	"fmt"
	"strings"
)

type ValidatorInfo struct {
	Type   string          `json:"type"`
	Params json.RawMessage `json:"params"`
}

type FieldMeta struct {
	Name       string          `json:"name"`
	Type       Types           `json:"type"`
	Length     int             `json:"length"`
	Required   bool            `json:"required,omitempty"`
	Validators []ValidatorInfo `json:"validators,omitempty"`
}

type FieldsMeta []FieldMeta

func (f FieldsMeta) String() string {
	var results []string
	for _, field := range f {
		result := fmt.Sprintf("Name: %s, Type: %s, Length: %d, Required: %t", field.Name, field.Type, field.Length, field.Required)
		results = append(results, result)
	}

	return fmt.Sprintf("[%s]", strings.Join(results, ", "))
}
