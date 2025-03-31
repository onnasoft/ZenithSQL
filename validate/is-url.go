package validate

import (
	"fmt"
	"net/url"
)

type IsURL struct{}

func (v IsURL) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string", colName)
	}
	_, err := url.ParseRequestURI(str)
	if err != nil {
		return fmt.Errorf("column '%s' must be a valid URL", colName)
	}
	return nil
}
