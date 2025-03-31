package validate

import (
	"fmt"
	"time"
)

type IsRFC3339 struct{}

func (v IsRFC3339) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string", colName)
	}
	if _, err := time.Parse(time.RFC3339, str); err != nil {
		return fmt.Errorf("column '%s' must be a valid RFC3339 timestamp", colName)
	}
	return nil
}
