package validate

import (
	"fmt"
	"net"
	"strings"
)

type IsExistingEmail struct{}

func (v IsExistingEmail) Validate(value interface{}, colName string) error {
	email, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsExistingEmail validation", colName)
	}

	at := strings.LastIndex(email, "@")
	if at == -1 || at == len(email)-1 {
		return fmt.Errorf("column '%s' is not a valid email format", colName)
	}

	domain := email[at+1:]
	_, err := net.LookupMX(domain)
	if err != nil {
		return fmt.Errorf("column '%s' email domain '%s' does not exist", colName, domain)
	}

	return nil
}
