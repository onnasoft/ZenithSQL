package types

import (
	"errors"
	"unsafe"
)

type Uint16Type struct{}

func (Uint16Type) ResolveLength(length int) (int, error) {
	return 2, nil
}

func (Uint16Type) Read(data []byte, out interface{}) error {
	if len(data) < 2 {
		return errors.New("insufficient data for Uint16 (need 2 bytes)")
	}
	ptr, ok := out.(*uint16)
	if !ok {
		return errors.New("output must be *uint16")
	}
	*ptr = *(*uint16)(unsafe.Pointer(&data[0]))
	return nil
}

func (Uint16Type) Write(buffer []byte, value interface{}) error {
	v, ok := value.(uint16)
	if !ok && value != nil {
		return errors.New("type assertion failed for Uint16")
	}
	*(*uint16)(unsafe.Pointer(&buffer[0])) = v
	return nil
}

func (Uint16Type) Parse(data []byte) interface{} {
	return *(*int16)(unsafe.Pointer(&data[0]))
}

func (Uint16Type) Valid(value interface{}) error {
	if _, ok := value.(uint16); !ok {
		return errors.New("value is not uint16")
	}
	return nil
}

func (Uint16Type) String() string {
	return "uint16"
}
