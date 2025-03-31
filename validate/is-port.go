package validate

import (
	"fmt"
	"strconv"
)

type IsPort struct{}

func (v IsPort) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string to validate as port", colName)
	}
	port, err := strconv.Atoi(str)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("column '%s' must be a valid port number (1-65535)", colName)
	}
	return nil
}
