package validate

import (
	"fmt"
)

type InRangeInt struct {
	Min int
	Max int
}

func (v InRangeInt) Validate(value interface{}, colName string) error {
	val, ok := value.(int)
	if !ok {
		return fmt.Errorf("column '%s' must be an int for InRangeInt validation", colName)
	}
	if val < v.Min || val > v.Max {
		return fmt.Errorf("column '%s' must be in range [%d, %d]", colName, v.Min, v.Max)
	}
	return nil
}
