package validate

import (
	"encoding/hex"
	"fmt"
)

type IsTiger128 struct{}

func (v IsTiger128) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok || len(str) != 32 {
		return fmt.Errorf("column '%s' must be a valid Tiger128 hash (32 hex chars)", colName)
	}
	_, err := hex.DecodeString(str)
	if err != nil {
		return fmt.Errorf("column '%s' must be a valid hexadecimal string", colName)
	}
	return nil
}
