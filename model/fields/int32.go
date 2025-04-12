package fields

import (
	"errors"
	"fmt"
	"unsafe"
)

type Int32Type struct{}

func (Int32Type) ResolveLength(length int) (int, error) {
	return 4, nil
}

func (dt Int32Type) Read(data []byte, out interface{}) error {
	if len(data) < 4 {
		return errors.New("insufficient data for Int32 (need 4 bytes)")
	}
	ptr, ok := out.(*int32)
	if !ok {
		return errors.New("output must be *int32")
	}
	*ptr = *(*int32)(unsafe.Pointer(&data[0]))
	return nil
}

func (dt Int32Type) Write(buffer []byte, value interface{}) error {
	v, ok := value.(int32)
	if !ok && value != nil {
		return fmt.Errorf("type assertion failed for Int32")
	}
	*(*int32)(unsafe.Pointer(&buffer[0])) = v
	return nil
}

func (dt Int32Type) Parse(data []byte) interface{} {
	return *(*int32)(unsafe.Pointer(&data[0]))
}

func (dt Int32Type) Valid(val interface{}) error {
	if _, ok := val.(int32); !ok {
		return errors.New("value is not of type int32")
	}
	return nil
}

func (Int32Type) String() string {
	return "Int32"
}
