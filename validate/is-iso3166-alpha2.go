package validate

import (
	"fmt"

	"github.com/asaskevich/govalidator"
)

type IsISO3166Alpha2 struct{}

func (v IsISO3166Alpha2) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok || !govalidator.IsISO3166Alpha2(str) {
		return fmt.Errorf("column '%s' must be a valid ISO 3166 Alpha-2 code", colName)
	}
	return nil
}
