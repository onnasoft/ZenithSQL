package validate

import (
	"fmt"
	"net"
)

type IsIPv6 struct{}

func (v IsIPv6) Validate(value interface{}, colName string) error {
	ip, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsIPv6 validation", colName)
	}
	parsed := net.ParseIP(ip)
	if parsed == nil || parsed.To16() == nil || parsed.To4() != nil {
		return fmt.Errorf("column '%s' is not a valid IPv6 address", colName)
	}
	return nil
}
