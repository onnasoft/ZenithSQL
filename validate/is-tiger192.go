package validate

import (
	"encoding/hex"
	"fmt"
)

type IsTiger192 struct{}

func (v IsTiger192) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok || len(str) != 48 {
		return fmt.Errorf("column '%s' must be a valid Tiger192 hash (48 hex chars)", colName)
	}
	_, err := hex.DecodeString(str)
	if err != nil {
		return fmt.Errorf("column '%s' must be a valid hexadecimal string", colName)
	}
	return nil
}
