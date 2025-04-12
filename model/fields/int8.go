package fields

import (
	"errors"
)

type Int8Type struct{}

func (Int8Type) String() string {
	return "Int8"
}

func (dt Int8Type) ResolveLength(length int) (int, error) {
	return 1, nil
}

func (dt Int8Type) Read(data []byte, out interface{}) error {
	if len(data) < 1 {
		return errors.New("insufficient data for Int8 (need 1 byte)")
	}
	ptr, ok := out.(*int8)
	if !ok {
		return errors.New("output must be *int8")
	}
	*ptr = int8(data[0])
	return nil
}

func (dt Int8Type) Write(buffer []byte, value interface{}) error {
	v, ok := value.(int8)
	if !ok && value != nil {
		return errors.New("type assertion failed for Int8")
	}
	buffer[0] = byte(v)
	return nil
}

func (dt Int8Type) Parse(data []byte) interface{} {
	if len(data) < 1 {
		return nil
	}
	return int8(data[0])
}

func (dt Int8Type) Valid(val interface{}) error {
	if _, ok := val.(int8); !ok {
		return errors.New("value is not of type int8")
	}
	return nil
}
