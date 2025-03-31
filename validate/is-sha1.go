package validate

import (
	"encoding/hex"
	"fmt"
)

type IsSHA1 struct{}

func (v IsSHA1) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok || len(str) != 40 {
		return fmt.Errorf("column '%s' must be a valid SHA1 hash (40 hex chars)", colName)
	}
	_, err := hex.DecodeString(str)
	if err != nil {
		return fmt.Errorf("column '%s' must be a valid hexadecimal string", colName)
	}
	return nil
}
