package validate

import (
	"fmt"
	"unicode"
)

type IsAlphanumeric struct{}

func (v IsAlphanumeric) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsAlphanumeric validation", colName)
	}
	for _, c := range str {
		if !unicode.IsLetter(c) && !unicode.IsDigit(c) {
			return fmt.Errorf("column '%s' must contain only alphanumeric characters", colName)
		}
	}
	return nil
}
