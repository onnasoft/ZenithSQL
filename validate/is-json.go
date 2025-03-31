package validate

import (
	"encoding/json"
	"fmt"
)

type IsJSON struct{}

func (v IsJSON) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsJSON validation", colName)
	}
	var js interface{}
	if err := json.Unmarshal([]byte(str), &js); err != nil {
		return fmt.Errorf("column '%s' must be a valid JSON string", colName)
	}
	return nil
}
