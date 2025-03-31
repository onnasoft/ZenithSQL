package validate

import (
	"fmt"
)

type IsMultibyte struct{}

func (v IsMultibyte) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsMultibyte validation", colName)
	}
	for _, r := range str {
		if r > 127 {
			return nil
		}
	}
	return fmt.Errorf("column '%s' must contain multibyte characters", colName)
}
