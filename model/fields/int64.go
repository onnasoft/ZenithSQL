package fields

import (
	"errors"
	"unsafe"
)

type Int64Type struct{}

func (Int64Type) ResolveLength(length int) (int, error) {
	return 8, nil
}

func (dt Int64Type) Read(data []byte, out interface{}) error {
	if len(data) < 8 {
		return errors.New("insufficient data for Int64 (need 8 bytes)")
	}
	ptr, ok := out.(*int64)
	if !ok {
		return errors.New("output must be *int64")
	}
	*ptr = *(*int64)(unsafe.Pointer(&data[0]))
	return nil
}

func (dt Int64Type) Write(buffer []byte, value interface{}) error {
	v, ok := value.(int64)
	if !ok && value != nil {
		return errors.New("type assertion failed for Int64")
	}
	*(*int64)(unsafe.Pointer(&buffer[0])) = v
	return nil
}

func (dt Int64Type) Parse(data []byte) interface{} {
	return *(*int64)(unsafe.Pointer(&data[0]))
}

func (dt Int64Type) Valid(val interface{}) error {
	if _, ok := val.(int64); !ok {
		return errors.New("value is not of type int64")
	}
	return nil
}

func (Int64Type) String() string {
	return "Int64"
}
