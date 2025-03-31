package validate

import (
	"encoding/hex"
	"fmt"
)

type IsSHA384 struct{}

func (v IsSHA384) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok || len(str) != 96 {
		return fmt.Errorf("column '%s' must be a valid SHA384 hash (96 hex chars)", colName)
	}
	_, err := hex.DecodeString(str)
	if err != nil {
		return fmt.Errorf("column '%s' must be a valid hexadecimal string", colName)
	}
	return nil
}
