package validate

import (
	"fmt"
	"regexp"
)

type IsHexadecimal struct{}

var hexRegex = regexp.MustCompile(`^[0-9a-fA-F]+$`)

func (v IsHexadecimal) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok || !hexRegex.MatchString(str) {
		return fmt.Errorf("column '%s' must be a valid hexadecimal string", colName)
	}
	return nil
}
