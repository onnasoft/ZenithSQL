package utils

import (
	"reflect"
)

func Clone(value interface{}) interface{} {
	val := reflect.ValueOf(value)
	if val.Kind().String() != "ptr" {
		return val
	}

	cloned := val.Elem().Interface()

	return cloned
}
