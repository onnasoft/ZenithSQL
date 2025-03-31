package validate

import (
	"fmt"

	"github.com/oklog/ulid/v2"
)

type IsULID struct{}

func (v IsULID) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string", colName)
	}
	_, err := ulid.Parse(str)
	if err != nil {
		return fmt.Errorf("column '%s' must be a valid ULID", colName)
	}
	return nil
}
