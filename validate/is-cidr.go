package validate

import (
	"fmt"
	"net"
)

type IsCIDR struct{}

func (v IsCIDR) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsCIDR validation", colName)
	}
	if _, _, err := net.ParseCIDR(str); err != nil {
		return fmt.Errorf("column '%s' must be a valid CIDR notation", colName)
	}
	return nil
}
