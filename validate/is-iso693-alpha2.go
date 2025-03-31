package validate

import (
	"fmt"

	"github.com/asaskevich/govalidator"
)

type IsISO693Alpha2 struct{}

func (v IsISO693Alpha2) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok || !govalidator.IsISO693Alpha2(str) {
		return fmt.Errorf("column '%s' must be a valid ISO 693 Alpha-2 language code", colName)
	}
	return nil
}
