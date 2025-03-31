package validate

import (
	"fmt"
)

type InRangeFloat64 struct {
	Min float64
	Max float64
}

func (v InRangeFloat64) Validate(value interface{}, colName string) error {
	val, ok := value.(float64)
	if !ok {
		return fmt.Errorf("column '%s' must be a float64 for InRangeFloat64 validation", colName)
	}
	if val < v.Min || val > v.Max {
		return fmt.Errorf("column '%s' must be in range [%f, %f]", colName, v.Min, v.Max)
	}
	return nil
}
