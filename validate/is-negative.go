package validate

import (
	"fmt"
)

type IsNegative struct{}

func (v IsNegative) Validate(value interface{}, colName string) error {
	num, ok := value.(float64)
	if !ok || num >= 0 {
		return fmt.Errorf("column '%s' must be a negative number", colName)
	}
	return nil
}
