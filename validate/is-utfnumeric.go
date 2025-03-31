package validate

import (
	"fmt"
	"unicode"
)

type IsUTFNumeric struct{}

func (v IsUTFNumeric) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string", colName)
	}
	for _, r := range str {
		if !unicode.IsNumber(r) {
			return fmt.Errorf("column '%s' must contain only numeric characters", colName)
		}
	}
	return nil
}
