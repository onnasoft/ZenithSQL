package validate

import (
	"fmt"
)

type IsNonNegative struct{}

func (v IsNonNegative) Validate(value interface{}, colName string) error {
	num, ok := value.(float64)
	if !ok || num < 0 {
		return fmt.Errorf("column '%s' must be a non-negative number", colName)
	}
	return nil
}
