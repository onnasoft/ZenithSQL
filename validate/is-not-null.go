package validate

import (
	"fmt"
)

type IsNotNull struct{}

func (v IsNotNull) Validate(value interface{}, colName string) error {
	if value == nil {
		return fmt.Errorf("column '%s' cannot be null", colName)
	}
	return nil
}
