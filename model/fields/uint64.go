package fields

import (
	"errors"
	"unsafe"
)

type Uint64Type struct{}

func (Uint64Type) ResolveLength(length int) (int, error) {
	return 8, nil
}

func (Uint64Type) Read(data []byte, out interface{}) error {
	if len(data) < 8 {
		return errors.New("insufficient data for Uint64 (need 8 bytes)")
	}
	ptr, ok := out.(*uint64)
	if !ok {
		return errors.New("output must be *uint64")
	}
	*ptr = *(*uint64)(unsafe.Pointer(&data[0]))
	return nil
}

func (Uint64Type) Write(buffer []byte, value interface{}) error {
	v, ok := value.(uint64)
	if !ok && value != nil {
		return errors.New("type assertion failed for Uint64")
	}
	*(*uint64)(unsafe.Pointer(&buffer[0])) = v
	return nil
}

func (Uint64Type) Parse(data []byte) interface{} {
	return *(*int64)(unsafe.Pointer(&data[0]))
}

func (Uint64Type) Valid(value interface{}) error {
	if _, ok := value.(uint64); !ok {
		return errors.New("value is not uint64")
	}
	return nil
}

func (Uint64Type) String() string {
	return "uint64"
}
