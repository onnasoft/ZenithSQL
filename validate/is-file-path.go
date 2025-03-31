package validate

import (
	"fmt"
	"os"
)

type IsFilePath struct{}

func (v IsFilePath) Validate(value interface{}, colName string) error {
	path, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsFilePath validation", colName)
	}
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return fmt.Errorf("column '%s' must be a valid file path", colName)
	}
	return nil
}
