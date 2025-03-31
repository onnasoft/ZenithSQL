package validate

import (
	"fmt"
)

type InRangeFloat32 struct {
	Min float32
	Max float32
}

func (v InRangeFloat32) Validate(value interface{}, colName string) error {
	val, ok := value.(float32)
	if !ok {
		return fmt.Errorf("column '%s' must be a float32 for InRangeFloat32 validation", colName)
	}
	if val < v.Min || val > v.Max {
		return fmt.Errorf("column '%s' must be in range [%f, %f]", colName, v.Min, v.Max)
	}
	return nil
}
