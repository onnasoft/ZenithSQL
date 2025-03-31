package validate

import (
	"fmt"
)

type IsHalfWidth struct{}

func (v IsHalfWidth) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsHalfWidth validation", colName)
	}
	for _, r := range str {
		if r > '\uFF00' && r < '\uFFEF' {
			return fmt.Errorf("column '%s' contains non-half-width characters", colName)
		}
	}
	return nil
}
