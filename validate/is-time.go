package validate

import (
	"fmt"
	"time"
)

type IsTime struct {
	Format string
}

func (v IsTime) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string", colName)
	}
	_, err := time.Parse(v.Format, str)
	if err != nil {
		return fmt.Errorf("column '%s' must match time format '%s'", colName, v.Format)
	}
	return nil
}
