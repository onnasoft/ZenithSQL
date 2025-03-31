package validate

import (
	"fmt"
	"unicode"
)

type IsUTFLetter struct{}

func (v IsUTFLetter) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string", colName)
	}
	for _, r := range str {
		if !unicode.IsLetter(r) {
			return fmt.Errorf("column '%s' must contain only letter characters", colName)
		}
	}
	return nil
}
