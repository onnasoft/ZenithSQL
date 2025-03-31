package validate

import (
	"fmt"

	"github.com/google/uuid"
)

type IsUUID struct{}

func (v IsUUID) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string", colName)
	}
	_, err := uuid.Parse(str)
	if err != nil {
		return fmt.Errorf("column '%s' must be a valid UUID", colName)
	}
	return nil
}
