package validate

import (
	"fmt"
)

type IsIn struct {
	Options []string
}

func (v IsIn) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsIn validation", colName)
	}
	for _, opt := range v.Options {
		if str == opt {
			return nil
		}
	}
	return fmt.Errorf("column '%s' must be one of %v", colName, v.Options)
}
