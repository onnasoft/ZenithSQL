package fields

import (
	"errors"
	"unsafe"
)

type Float32Type struct{}

func (Float32Type) ResolveLength(length int) (int, error) {
	return 4, nil
}

func (Float32Type) Read(data []byte, out interface{}) error {
	if len(data) < 4 {
		return errors.New("insufficient data for Float32 (need 4 bytes)")
	}
	ptr, ok := out.(*float32)
	if !ok {
		return errors.New("output must be *float32")
	}
	*ptr = *(*float32)(unsafe.Pointer(&data[0]))
	return nil
}

func (Float32Type) Write(buffer []byte, value interface{}) error {
	v, ok := value.(float32)
	if !ok && value != nil {
		return errors.New("type assertion failed for Float32")
	}
	*(*float32)(unsafe.Pointer(&buffer[0])) = v
	return nil
}

func (Float32Type) Parse(data []byte) interface{} {
	if len(data) < 4 {
		return nil
	}
	return *(*float32)(unsafe.Pointer(&data[0]))
}

func (Float32Type) Valid(value interface{}) error {
	if _, ok := value.(float32); !ok {
		return errors.New("value is not float32")
	}
	return nil
}

func (Float32Type) String() string {
	return "float32"
}
