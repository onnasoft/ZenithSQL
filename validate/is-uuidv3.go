package validate

import (
	"fmt"

	"github.com/google/uuid"
)

type IsUUIDv3 struct{}

func (v IsUUIDv3) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string", colName)
	}
	u, err := uuid.Parse(str)
	if err != nil || u.Version() != 3 {
		return fmt.Errorf("column '%s' must be a valid UUIDv3", colName)
	}
	return nil
}
