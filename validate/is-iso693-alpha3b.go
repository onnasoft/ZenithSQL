package validate

import (
	"fmt"

	"github.com/asaskevich/govalidator"
)

type IsISO693Alpha3b struct{}

func (v IsISO693Alpha3b) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok || !govalidator.IsISO693Alpha3b(str) {
		return fmt.Errorf("column '%s' must be a valid ISO 693 Alpha-3b language code", colName)
	}
	return nil
}
