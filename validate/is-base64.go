package validate

import (
	"encoding/base64"
	"fmt"
)

type IsBase64 struct{}

func (v IsBase64) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsBase64 validation", colName)
	}
	_, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return fmt.Errorf("column '%s' must be a valid base64 string", colName)
	}
	return nil
}
