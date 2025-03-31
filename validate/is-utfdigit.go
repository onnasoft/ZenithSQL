package validate

import (
	"fmt"
	"unicode"
)

type IsUTFDigit struct{}

func (v IsUTFDigit) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string", colName)
	}
	for _, r := range str {
		if !unicode.IsDigit(r) {
			return fmt.Errorf("column '%s' must contain only digit characters", colName)
		}
	}
	return nil
}
