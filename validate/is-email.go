package validate

import (
	"fmt"
	"net/mail"
	"strings"
)

var (
	errNotString    = fmt.Errorf("must be a string")
	errInvalidEmail = fmt.Errorf("must be a valid email address")
)

type IsEmail struct{}

func (v IsEmail) Validate(value interface{}, colName string) error {
	const msg = "column '%s' %w"
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf(msg, colName, errNotString)
	}

	if len(str) > 254 || len(str) < 3 {
		return fmt.Errorf(msg, colName, errInvalidEmail)
	}

	if !strings.Contains(str, "@") {
		return fmt.Errorf(msg, colName, errInvalidEmail)
	}

	_, err := mail.ParseAddress(str)
	if err != nil {
		return fmt.Errorf(msg, colName, errInvalidEmail)
	}

	return nil
}

func (v IsEmail) Type() string {
	return "isEmail"
}

func (v IsEmail) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"type": "isEmail",
	}
}

func (v *IsEmail) FromMap(value map[string]interface{}) {

}
