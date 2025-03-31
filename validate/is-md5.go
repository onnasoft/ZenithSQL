package validate

import (
	"encoding/hex"
	"fmt"
)

type IsMD5 struct{}

func (v IsMD5) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok || len(str) != 32 {
		return fmt.Errorf("column '%s' must be a valid MD5 hash", colName)
	}
	_, err := hex.DecodeString(str)
	if err != nil {
		return fmt.Errorf("column '%s' is not a valid hexadecimal MD5", colName)
	}
	return nil
}
