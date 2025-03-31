package validate

import (
	"fmt"
	"regexp"
)

type IsSSN struct{}

var ssnRegex = regexp.MustCompile(`^\d{3}-\d{2}-\d{4}$`)

func (v IsSSN) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok || !ssnRegex.MatchString(str) {
		return fmt.Errorf("column '%s' must be a valid SSN (format: XXX-XX-XXXX)", colName)
	}
	return nil
}
