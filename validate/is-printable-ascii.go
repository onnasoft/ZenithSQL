package validate

import (
	"fmt"
)

type IsPrintableASCII struct{}

func (v IsPrintableASCII) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string", colName)
	}
	for _, r := range str {
		if r < 32 || r > 126 {
			return fmt.Errorf("column '%s' contains non-printable ASCII characters", colName)
		}
	}
	return nil
}
