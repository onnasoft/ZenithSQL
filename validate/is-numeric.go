package validate

import (
	"fmt"
	"unicode"
)

type IsNumeric struct{}

func (v IsNumeric) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for numeric check", colName)
	}
	for _, r := range str {
		if !unicode.IsDigit(r) {
			return fmt.Errorf("column '%s' must be numeric", colName)
		}
	}
	return nil
}
