package validate

import (
	"fmt"
	"unicode"
)

type HasWhitespace struct{}

func (v HasWhitespace) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for HasWhitespace validation", colName)
	}
	for _, c := range str {
		if unicode.IsSpace(c) {
			return nil
		}
	}
	return fmt.Errorf("column '%s' must contain at least one whitespace character", colName)
}
