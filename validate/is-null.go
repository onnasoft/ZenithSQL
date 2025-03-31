package validate

import (
	"fmt"
)

type IsNull struct{}

func (v IsNull) Validate(value interface{}, colName string) error {
	if value != nil {
		return fmt.Errorf("column '%s' must be null", colName)
	}
	return nil
}
