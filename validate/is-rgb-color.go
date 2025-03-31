package validate

import (
	"fmt"
	"regexp"
)

type IsRGBcolor struct{}

var rgbRegex = regexp.MustCompile(`^rgb\(\s*(?:[01]?\d{1,2}|2[0-4]\d|25[0-5])\s*,\s*(?:[01]?\d{1,2}|2[0-4]\d|25[0-5])\s*,\s*(?:[01]?\d{1,2}|2[0-4]\d|25[0-5])\s*\)$`)

func (v IsRGBcolor) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok || !rgbRegex.MatchString(str) {
		return fmt.Errorf("column '%s' must be a valid RGB color (e.g., rgb(255,255,255))", colName)
	}
	return nil
}
