package validate

import (
	"fmt"

	"github.com/asaskevich/govalidator"
)

type IsISBN10 struct{}

func (v IsISBN10) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok || !govalidator.IsISBN(str, 10) {
		return fmt.Errorf("column '%s' must be a valid ISBN-10", colName)
	}
	return nil
}
