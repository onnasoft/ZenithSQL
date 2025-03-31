package validate

import (
	"fmt"

	"github.com/google/uuid"
)

type IsUUIDv5 struct{}

func (v IsUUIDv5) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string", colName)
	}
	u, err := uuid.Parse(str)
	if err != nil || u.Version() != 5 {
		return fmt.Errorf("column '%s' must be a valid UUIDv5", colName)
	}
	return nil
}
