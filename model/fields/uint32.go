package fields

import (
	"errors"
	"unsafe"
)

type Uint32Type struct{}

func (Uint32Type) ResolveLength(length int) (int, error) {
	return 4, nil
}

func (Uint32Type) Read(data []byte, out interface{}) error {
	if len(data) < 4 {
		return errors.New("insufficient data for Uint32 (need 4 bytes)")
	}
	ptr, ok := out.(*uint32)
	if !ok {
		return errors.New("output must be *uint32")
	}
	*ptr = *(*uint32)(unsafe.Pointer(&data[0]))
	return nil
}

func (Uint32Type) Write(buffer []byte, value interface{}) error {
	v, ok := value.(uint32)
	if !ok && value != nil {
		return errors.New("type assertion failed for Uint32")
	}
	*(*uint32)(unsafe.Pointer(&buffer[0])) = v
	return nil
}

func (Uint32Type) Parse(data []byte) interface{} {
	return *(*uint32)(unsafe.Pointer(&data[0]))
}

func (Uint32Type) Valid(value interface{}) error {
	if _, ok := value.(uint32); !ok {
		return errors.New("value is not uint32")
	}
	return nil
}

func (Uint32Type) String() string {
	return "uint32"
}
