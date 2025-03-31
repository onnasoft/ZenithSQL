package validate

import (
	"fmt"
	"time"
)

type IsYYYYMMDD struct{}

func (v IsYYYYMMDD) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string", colName)
	}
	_, err := time.Parse("20060102", str)
	if err != nil {
		return fmt.Errorf("column '%s' must be a valid date in YYYYMMDD format", colName)
	}
	return nil
}
