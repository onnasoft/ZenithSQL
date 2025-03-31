package validate

import (
	"fmt"
)

type IsNatural struct{}

func (v IsNatural) Validate(value interface{}, colName string) error {
	num, ok := value.(float64)
	if !ok || num < 0 || num != float64(int64(num)) {
		return fmt.Errorf("column '%s' must be a natural number", colName)
	}
	return nil
}
