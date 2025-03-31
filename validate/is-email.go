package validate

import (
	"fmt"
	"net/mail"
)

type IsEmail struct{}

func (v IsEmail) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsEmail validation", colName)
	}
	_, err := mail.ParseAddress(str)
	if err != nil {
		return fmt.Errorf("column '%s' must be a valid email address", colName)
	}
	return nil
}
