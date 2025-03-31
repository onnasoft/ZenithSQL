package validate

import (
	"fmt"
	"net"
)

type IsIP struct{}

func (v IsIP) Validate(value interface{}, colName string) error {
	ip, ok := value.(string)
	if !ok || net.ParseIP(ip) == nil {
		return fmt.Errorf("column '%s' must be a valid IP address", colName)
	}
	return nil
}
