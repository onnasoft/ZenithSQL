package entity

import (
	"fmt"
	"log"

	"github.com/onnasoft/ZenithSQL/validate"
)

type Field struct {
	Name            string              `json:"name"`
	Type            DataType            `json:"type"`
	Length          int                 `json:"length"`
	Validators      validate.Validators `json:"validators"`
	IsSettedFlagPos int                 `json:"is_setted_flag_pos"`
	StartPosition   int                 `json:"start_position"`
	EndPosition     int                 `json:"end_position"`
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

func (f *Field) Prepare(offset int) {
	if f.Length <= 0 {
		log.Fatalf("invalid length %d for field %s", f.Length, f.Name)
	}

	f.IsSettedFlagPos = offset
	f.StartPosition = offset + 1
	f.EndPosition = f.StartPosition + f.Length
}

func (f *Field) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"name":               f.Name,
		"type":               f.Type.String(),
		"length":             f.Length,
		"validators":         f.Validators.ToMap(),
		"is_setted_flag_pos": f.IsSettedFlagPos,
		"start_position":     f.StartPosition,
		"end_position":       f.EndPosition,
	}
}

func (f *Field) FromMap(v map[string]interface{}) error {
	errMsg := "field %v is required"

	name, ok := v["name"].(string)
	if !ok {
		return fmt.Errorf(errMsg, "name")
	}
	f.Name = name

	fieldType, ok := v["type"].(string)
	if !ok {
		return fmt.Errorf(errMsg, "type")
	}
	f.Type = DataTypeFromString(fieldType)

	length, ok := v["length"].(float64)
	if !ok {
		return fmt.Errorf(errMsg, "length")
	}
	f.Length = int(length)

	isSettedFlagPos, ok := v["is_setted_flag_pos"].(float64)
	if !ok {
		return fmt.Errorf(errMsg, "is_setted_flag_pos")
	}
	f.IsSettedFlagPos = int(isSettedFlagPos)

	startPosition, ok := v["start_position"].(float64)
	if !ok {
		return fmt.Errorf(errMsg, "start_position")
	}
	f.StartPosition = int(startPosition)

	endPosition, ok := v["end_position"].(float64)
	if !ok {
		return fmt.Errorf(errMsg, "end_position")
	}
	f.EndPosition = int(endPosition)

	validators, ok := v["validators"].([]interface{})
	if !ok {
		return fmt.Errorf(errMsg, "validators")
	}
	if len(validators) == 0 {
		return nil
	}

	f.Validators = make(validate.Validators, len(validators))
	for i := 0; i < len(validators); i++ {
		validator := validators[i].(map[string]interface{})
		f.Validators[i], _ = validate.FromMap(validator)
	}

	return nil
}
