package fields

import (
	"errors"
	"strings"
)

type StringType struct{}

func (dt StringType) ResolveLength(length int) (int, error) {
	if length > 0 {
		return length, nil
	}
	return 255, nil
}

func (dt StringType) Read(data []byte, out interface{}) error {
	if len(data) == 0 {
		return nil
	}
	ptr, ok := out.(*string)
	if !ok {
		return errors.New("output must be *string")
	}
	*ptr = strings.TrimRight(string(data), "\x00")
	return nil
}

func (dt StringType) Write(buffer []byte, value interface{}) error {
	v, ok := value.(string)
	if !ok && value != nil {
		return errors.New("type assertion failed for String")
	}
	copy(buffer, v)
	return nil
}

func (dt StringType) Parse(data []byte) interface{} {
	return strings.TrimRight(string(data), "\x00")
}

func (dt StringType) Valid(val interface{}) error {
	if _, ok := val.(string); !ok {
		return errors.New("value is not of type string")
	}
	return nil
}

func (dt StringType) String() string {
	return "string"
}
