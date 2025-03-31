package validate

import (
	"fmt"
)

type IsByteLength struct {
	Min int
	Max int
}

func (v IsByteLength) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsByteLength validation", colName)
	}
	length := len([]byte(str))
	if length < v.Min || length > v.Max {
		return fmt.Errorf("column '%s' must have byte length between %d and %d", colName, v.Min, v.Max)
	}
	return nil
}
