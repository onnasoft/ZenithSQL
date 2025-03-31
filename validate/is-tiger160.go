package validate

import (
	"encoding/hex"
	"fmt"
)

type IsTiger160 struct{}

func (v IsTiger160) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok || len(str) != 40 {
		return fmt.Errorf("column '%s' must be a valid Tiger160 hash (40 hex chars)", colName)
	}
	_, err := hex.DecodeString(str)
	if err != nil {
		return fmt.Errorf("column '%s' must be a valid hexadecimal string", colName)
	}
	return nil
}
