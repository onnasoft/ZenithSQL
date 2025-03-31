package validate

import (
	"fmt"

	"github.com/google/uuid"
)

type IsUUIDv4 struct{}

func (v IsUUIDv4) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string", colName)
	}
	u, err := uuid.Parse(str)
	if err != nil || u.Version() != 4 {
		return fmt.Errorf("column '%s' must be a valid UUIDv4", colName)
	}
	return nil
}
