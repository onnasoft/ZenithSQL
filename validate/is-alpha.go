package validate

import (
	"fmt"
)

type IsAlpha struct{}

func (v IsAlpha) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsAlpha validation", colName)
	}

	if len(str) == 0 {
		return fmt.Errorf("column '%s' must not be empty", colName)
	}

	for _, c := range str {
		// Solo permite A-Z, a-z
		if (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') {
			return fmt.Errorf("column '%s' must contain only ASCII alphabetic characters", colName)
		}
	}
	return nil
}
