package validate

import (
	"fmt"
	"regexp"
)

type IsHexcolor struct{}

var hexColorRegex = regexp.MustCompile(`^#?([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$`)

func (v IsHexcolor) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok || !hexColorRegex.MatchString(str) {
		return fmt.Errorf("column '%s' must be a valid hex color", colName)
	}
	return nil
}
