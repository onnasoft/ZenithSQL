package validate

import (
	"fmt"
	"regexp"
)

type IsDataURI struct{}

var dataURIRegex = regexp.MustCompile(`^data:.+/.+;base64,[a-zA-Z0-9+/]+={0,2}$`)

func (v IsDataURI) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok || !dataURIRegex.MatchString(str) {
		return fmt.Errorf("column '%s' must be a valid data URI", colName)
	}
	return nil
}
