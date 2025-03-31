package validate

import (
	"fmt"
	"reflect"
)

type IsType struct {
	Expected string
}

func (v IsType) Validate(value interface{}, colName string) error {
	actual := reflect.TypeOf(value).String()
	if actual != v.Expected {
		return fmt.Errorf("column '%s' expected type '%s', got '%s'", colName, v.Expected, actual)
	}
	return nil
}
