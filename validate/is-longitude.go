package validate

import (
	"fmt"
	"strconv"
)

type IsLongitude struct{}

func (v IsLongitude) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsLongitude validation", colName)
	}
	lon, err := strconv.ParseFloat(str, 64)
	if err != nil || lon < -180 || lon > 180 {
		return fmt.Errorf("column '%s' must be a valid longitude (-180 to 180)", colName)
	}
	return nil
}
