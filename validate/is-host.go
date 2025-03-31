package validate

import (
	"fmt"
	"net"
)

type IsHost struct{}

func (v IsHost) Validate(value interface{}, colName string) error {
	host, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsHost validation", colName)
	}
	if _, err := net.LookupHost(host); err != nil {
		return fmt.Errorf("column '%s' is not a valid host: %v", colName, err)
	}
	return nil
}
