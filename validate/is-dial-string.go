package validate

import (
	"fmt"
	"net"
)

type IsDialString struct{}

func (v IsDialString) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsDialString validation", colName)
	}
	_, err := net.Dial("tcp", str)
	if err != nil {
		return fmt.Errorf("column '%s' is not a valid dial string: %v", colName, err)
	}
	return nil
}
