package validate

import (
	"fmt"
	"regexp"
)

type IsRegex struct{}

func (v IsRegex) Validate(value interface{}, colName string) error {
	_, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for regex validation", colName)
	}
	_, err := regexp.Compile(value.(string))
	if err != nil {
		return fmt.Errorf("column '%s' must be a valid regular expression", colName)
	}
	return nil
}
