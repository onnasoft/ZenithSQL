package validate

import (
	"fmt"
)

type IsFullWidth struct{}

func (v IsFullWidth) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsFullWidth validation", colName)
	}
	for _, r := range str {
		if r < 0xFF01 || r > 0xFF60 {
			return fmt.Errorf("column '%s' contains non-full-width characters", colName)
		}
	}
	return nil
}
