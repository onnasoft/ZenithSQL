package validate

import (
	"fmt"
	"unicode"
)

type IsUpperCase struct{}

func (v IsUpperCase) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string", colName)
	}
	for _, r := range str {
		if unicode.IsLower(r) {
			return fmt.Errorf("column '%s' must be uppercase", colName)
		}
	}
	return nil
}
