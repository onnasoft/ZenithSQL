package validate

import (
	"fmt"
)

type IsPositive struct{}

func (v IsPositive) Validate(value interface{}, colName string) error {
	num, ok := value.(float64)
	if !ok || num <= 0 {
		return fmt.Errorf("column '%s' must be a positive number", colName)
	}
	return nil
}
