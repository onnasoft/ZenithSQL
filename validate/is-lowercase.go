package validate

import (
	"fmt"
	"strings"
)

type IsLowerCase struct{}

func (v IsLowerCase) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsLowerCase validation", colName)
	}
	if str != strings.ToLower(str) {
		return fmt.Errorf("column '%s' must be lowercase", colName)
	}
	return nil
}
