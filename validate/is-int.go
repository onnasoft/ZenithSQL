package validate

import (
	"fmt"
	"strconv"
)

type IsInt struct{}

func (v IsInt) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsInt validation", colName)
	}
	if _, err := strconv.Atoi(str); err != nil {
		return fmt.Errorf("column '%s' must be an integer", colName)
	}
	return nil
}
