package validate

import (
	"fmt"

	"github.com/asaskevich/govalidator"
)

type IsISO4217 struct{}

func (v IsISO4217) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok || !govalidator.IsISO4217(str) {
		return fmt.Errorf("column '%s' must be a valid ISO 4217 currency code", colName)
	}
	return nil
}
