package fields

import (
	"errors"
	"unsafe"
)

type Int16Type struct{}

func (Int16Type) String() string {
	return "Int16"
}

func (Int16Type) ResolveLength(length int) (int, error) {
	return 2, nil
}

func (dt Int16Type) Read(data []byte, out interface{}) error {
	if len(data) < 2 {
		return errors.New("insufficient data for Int16 (need 2 bytes)")
	}
	ptr, ok := out.(*int16)
	if !ok {
		return errors.New("output must be *int16")
	}
	*ptr = *(*int16)(unsafe.Pointer(&data[0]))
	return nil
}

func (dt Int16Type) Write(buffer []byte, value interface{}) error {
	v, ok := value.(int16)
	if !ok && value != nil {
		return errors.New("type assertion failed for Int16")
	}
	*(*int16)(unsafe.Pointer(&buffer[0])) = v
	return nil
}

func (dt Int16Type) Parse(data []byte) interface{} {
	if len(data) < 2 {
		return nil
	}
	return int16(data[0]) | int16(data[1])<<8
}

func (dt Int16Type) Valid(val interface{}) error {
	if _, ok := val.(int16); !ok {
		return errors.New("value is not of type int16")
	}
	return nil
}
