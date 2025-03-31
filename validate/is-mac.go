package validate

import (
	"fmt"
	"net"
)

type IsMAC struct{}

func (v IsMAC) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsMAC validation", colName)
	}
	if _, err := net.ParseMAC(str); err != nil {
		return fmt.Errorf("column '%s' must be a valid MAC address", colName)
	}
	return nil
}
