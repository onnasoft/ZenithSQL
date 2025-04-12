package fields

import (
	"errors"
	"unsafe"
)

type Float64Type struct{}

func (Float64Type) ResolveLength(length int) (int, error) {
	return 8, nil
}

func (Float64Type) String() string {
	return "Float64"
}

func (dt Float64Type) Read(data []byte, out interface{}) error {
	if len(data) < 8 {
		return errors.New("insufficient data for Float64 (need 8 bytes)")
	}
	ptr, ok := out.(*float64)
	if !ok {
		return errors.New("output must be *float64")
	}
	*ptr = *(*float64)(unsafe.Pointer(&data[0]))
	return nil
}

func (dt Float64Type) Write(buffer []byte, value interface{}) error {
	v, ok := value.(float64)
	if !ok && value != nil {
		return errors.New("type assertion failed for Float64")
	}
	*(*float64)(unsafe.Pointer(&buffer[0])) = v
	return nil
}

func (dt Float64Type) Parse(data []byte) interface{} {
	if len(data) < 8 {
		return nil
	}
	return *(*float64)(unsafe.Pointer(&data[0]))
}

func (dt Float64Type) Valid(val interface{}) error {
	if _, ok := val.(float64); !ok {
		return errors.New("value is not of type float64")
	}
	return nil
}
