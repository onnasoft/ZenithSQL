package validate

import (
	"fmt"
	"unicode"
)

type HasWhitespaceOnly struct{}

func (v HasWhitespaceOnly) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for HasWhitespaceOnly validation", colName)
	}
	if str == "" {
		return fmt.Errorf("column '%s' cannot be empty", colName)
	}
	for _, c := range str {
		if !unicode.IsSpace(c) {
			return fmt.Errorf("column '%s' must contain only whitespace characters", colName)
		}
	}
	return nil
}
