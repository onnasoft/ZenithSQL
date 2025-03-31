package validate

import (
	"encoding/hex"
	"fmt"
)

type IsSHA512 struct{}

func (v IsSHA512) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok || len(str) != 128 {
		return fmt.Errorf("column '%s' must be a valid SHA512 hash (128 hex chars)", colName)
	}
	_, err := hex.DecodeString(str)
	if err != nil {
		return fmt.Errorf("column '%s' must be a valid hexadecimal string", colName)
	}
	return nil
}
