package validate

import (
	"fmt"
	"net/url"
)

type IsRequestURI struct{}

func (v IsRequestURI) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for request URI validation", colName)
	}
	if _, err := url.ParseRequestURI(str); err != nil {
		return fmt.Errorf("column '%s' must be a valid request URI", colName)
	}
	return nil
}
