package validate

import (
	"fmt"
	"net/url"
)

type IsRequestURL struct{}

func (v IsRequestURL) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for request URL validation", colName)
	}
	if _, err := url.Parse(str); err != nil {
		return fmt.Errorf("column '%s' must be a valid URL", colName)
	}
	return nil
}
