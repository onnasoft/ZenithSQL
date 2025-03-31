package validate

import (
	"fmt"
)

type IsInRaw struct {
	Options []string
}

func (v IsInRaw) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsInRaw validation", colName)
	}
	for _, opt := range v.Options {
		if str == opt {
			return nil
		}
	}
	return fmt.Errorf("column '%s' must match one of the raw values %v", colName, v.Options)
}
