package validate

import (
	"fmt"

	"github.com/asaskevich/govalidator"
)

type IsISBN struct {
	Version int
}

func (v IsISBN) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok || !govalidator.IsISBN(str, v.Version) {
		return fmt.Errorf("column '%s' must be a valid ISBN-%d", colName, v.Version)
	}
	return nil
}
