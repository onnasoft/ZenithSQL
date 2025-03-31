package validate

import (
	"fmt"
	"strconv"
)

type IsUnixTime struct{}

func (v IsUnixTime) Validate(value interface{}, colName string) error {
	switch val := value.(type) {
	case int64:
		if val < 0 {
			return fmt.Errorf("column '%s' must be a valid positive Unix time", colName)
		}
	case string:
		i, err := strconv.ParseInt(val, 10, 64)
		if err != nil || i < 0 {
			return fmt.Errorf("column '%s' must be a valid Unix time string", colName)
		}
	default:
		return fmt.Errorf("column '%s' must be an int64 or numeric string", colName)
	}
	return nil
}
