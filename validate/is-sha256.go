package validate

import (
	"encoding/hex"
	"fmt"
)

type IsSHA256 struct{}

func (v IsSHA256) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok || len(str) != 64 {
		return fmt.Errorf("column '%s' must be a valid SHA256 hash (64 hex chars)", colName)
	}
	_, err := hex.DecodeString(str)
	if err != nil {
		return fmt.Errorf("column '%s' must be a valid hexadecimal string", colName)
	}
	return nil
}
