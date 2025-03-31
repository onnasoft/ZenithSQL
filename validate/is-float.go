package validate

import (
	"fmt"
	"strconv"
)

type IsFloat struct{}

func (v IsFloat) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsFloat validation", colName)
	}
	if _, err := strconv.ParseFloat(str, 64); err != nil {
		return fmt.Errorf("column '%s' is not a valid float: %v", colName, err)
	}
	return nil
}
