package validate

import (
	"encoding/hex"
	"fmt"
)

type IsRipeMD160 struct{}

func (v IsRipeMD160) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok || len(str) != 40 {
		return fmt.Errorf("column '%s' must be a valid RipeMD160 hash (40 hex chars)", colName)
	}
	_, err := hex.DecodeString(str)
	if err != nil {
		return fmt.Errorf("column '%s' must be a valid hexadecimal string", colName)
	}
	return nil
}
