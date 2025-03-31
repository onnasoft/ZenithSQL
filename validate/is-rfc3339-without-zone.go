package validate

import (
	"fmt"
	"time"
)

type IsRFC3339WithoutZone struct{}

func (v IsRFC3339WithoutZone) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string", colName)
	}
	layout := "2006-01-02T15:04:05"
	if _, err := time.Parse(layout, str); err != nil {
		return fmt.Errorf("column '%s' must be a valid RFC3339 timestamp without zone", colName)
	}
	return nil
}
