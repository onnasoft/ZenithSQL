package validate

import (
	"fmt"
	"unicode"
)

type HasLowerCase struct{}

func (v HasLowerCase) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for HasLowerCase validation", colName)
	}
	for _, c := range str {
		if unicode.IsLower(c) {
			return nil
		}
	}
	return fmt.Errorf("column '%s' must contain at least one lowercase letter", colName)
}
