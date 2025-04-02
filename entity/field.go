package entity

import (
	"fmt"
	"log"

	"github.com/onnasoft/ZenithSQL/validate"
)

type Field struct {
	Name            string               `json:"name"`
	Type            DataType             `json:"type"`
	Length          int                  `json:"length"`
	Validators      []validate.Validator `json:"validators"`
	IsSettedFlagPos int                  `json:"is_setted_flag_pos"`
	StartPosition   int                  `json:"start_position"`
	EndPosition     int                  `json:"end_position"`
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

func (f *Field) Prepare(offset int) {
	if f.Length <= 0 {
		log.Fatalf("invalid length %d for field %s", f.Length, f.Name)
	}

	f.IsSettedFlagPos = offset
	f.StartPosition = offset + 1 // El byte después del flag
	f.EndPosition = f.StartPosition + f.Length
}
