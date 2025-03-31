package validate

import (
	"fmt"
)

type IsASCII struct{}

func (v IsASCII) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsASCII validation", colName)
	}
	for _, c := range str {
		if c > 127 {
			return fmt.Errorf("column '%s' must contain only ASCII characters", colName)
		}
	}
	return nil
}
