package validate

import (
	"fmt"
	"strconv"
)

type IsLatitude struct{}

func (v IsLatitude) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsLatitude validation", colName)
	}
	lat, err := strconv.ParseFloat(str, 64)
	if err != nil || lat < -90 || lat > 90 {
		return fmt.Errorf("column '%s' must be a valid latitude (-90 to 90)", colName)
	}
	return nil
}
