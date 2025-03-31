package validate

import (
	"fmt"
	"unicode"
)

type IsUTFLetterNumeric struct{}

func (v IsUTFLetterNumeric) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string", colName)
	}
	for _, r := range str {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return fmt.Errorf("column '%s' must contain only letters or digits", colName)
		}
	}
	return nil
}
