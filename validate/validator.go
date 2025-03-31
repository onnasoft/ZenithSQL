package validate

import (
	"fmt"
)

type Validator interface {
	Validate(value interface{}, colName string) error
}
type NotNull struct{}

func (v NotNull) Validate(value interface{}, colName string) error {
	if value == nil {
		return fmt.Errorf("column '%s' cannot be null", colName)
	}
	return nil
}
