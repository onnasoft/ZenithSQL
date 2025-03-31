package validate

import (
	"fmt"

	"github.com/asaskevich/govalidator"
)

type IsISBN13 struct{}

func (v IsISBN13) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok || !govalidator.IsISBN(str, 13) {
		return fmt.Errorf("column '%s' must be a valid ISBN-13", colName)
	}
	return nil
}
