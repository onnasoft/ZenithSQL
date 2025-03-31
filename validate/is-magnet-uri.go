package validate

import (
	"fmt"
	"strings"
)

type IsMagnetURI struct{}

func (v IsMagnetURI) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok || !strings.HasPrefix(str, "magnet:?") {
		return fmt.Errorf("column '%s' must be a valid Magnet URI", colName)
	}
	return nil
}
