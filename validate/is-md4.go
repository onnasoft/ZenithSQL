package validate

import (
	"encoding/hex"
	"fmt"
)

type IsMD4 struct{}

func (v IsMD4) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok || len(str) != 32 {
		return fmt.Errorf("column '%s' must be a valid MD4 hash", colName)
	}
	_, err := hex.DecodeString(str)
	if err != nil {
		return fmt.Errorf("column '%s' is not a valid hexadecimal MD4", colName)
	}
	// No native md4 in crypto; optionally validate format only
	return nil
}
