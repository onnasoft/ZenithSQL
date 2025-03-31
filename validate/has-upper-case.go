package validate

import (
	"fmt"
	"unicode"
)

type HasUpperCase struct{}

func (v HasUpperCase) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for HasUpperCase validation", colName)
	}
	for _, c := range str {
		if unicode.IsUpper(c) {
			return nil
		}
	}
	return fmt.Errorf("column '%s' must contain at least one uppercase letter", colName)
}
