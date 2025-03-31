package validate

import (
	"fmt"
)

type IsNonPositive struct{}

func (v IsNonPositive) Validate(value interface{}, colName string) error {
	num, ok := value.(float64)
	if !ok || num > 0 {
		return fmt.Errorf("column '%s' must be a non-positive number", colName)
	}
	return nil
}
