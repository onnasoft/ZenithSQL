package validate

import (
	"fmt"
	"net"
)

type IsIPv4 struct{}

func (v IsIPv4) Validate(value interface{}, colName string) error {
	ip, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsIPv4 validation", colName)
	}
	parsed := net.ParseIP(ip)
	if parsed == nil || parsed.To4() == nil {
		return fmt.Errorf("column '%s' is not a valid IPv4 address", colName)
	}
	return nil
}
