package types

import (
	"errors"
	"strings"
	"time"
	"unsafe"
)

// ReaderFunc es el tipo de función que lee datos en una referencia
type ReaderFunc func([]byte, interface{}) error

var ReaderTypes = map[DataType]ReaderFunc{
	Int8Type: func(data []byte, out interface{}) error {
		if len(data) < 1 {
			return errors.New("insufficient data for Int8 (need 1 byte)")
		}
		ptr, ok := out.(*int8)
		if !ok {
			return errors.New("output must be *int8")
		}
		*ptr = int8(data[0])
		return nil
	},

	Int16Type: func(data []byte, out interface{}) error {
		if len(data) < 2 {
			return errors.New("insufficient data for Int16 (need 2 bytes)")
		}
		ptr, ok := out.(*int16)
		if !ok {
			return errors.New("output must be *int16")
		}
		*ptr = *(*int16)(unsafe.Pointer(&data[0]))
		return nil
	},

	Int32Type: func(data []byte, out interface{}) error {
		if len(data) < 4 {
			return errors.New("insufficient data for Int32 (need 4 bytes)")
		}
		ptr, ok := out.(*int32)
		if !ok {
			return errors.New("output must be *int32")
		}
		*ptr = *(*int32)(unsafe.Pointer(&data[0]))
		return nil
	},

	Int64Type: func(data []byte, out interface{}) error {
		if len(data) < 8 {
			return errors.New("insufficient data for Int64 (need 8 bytes)")
		}
		ptr, ok := out.(*int64)
		if !ok {
			return errors.New("output must be *int64")
		}
		*ptr = *(*int64)(unsafe.Pointer(&data[0]))
		return nil
	},

	Uint8Type: func(data []byte, out interface{}) error {
		if len(data) < 1 {
			return errors.New("insufficient data for Uint8 (need 1 byte)")
		}
		ptr, ok := out.(*uint8)
		if !ok {
			return errors.New("output must be *uint8")
		}
		*ptr = data[0]
		return nil
	},

	Uint16Type: func(data []byte, out interface{}) error {
		if len(data) < 2 {
			return errors.New("insufficient data for Uint16 (need 2 bytes)")
		}
		ptr, ok := out.(*uint16)
		if !ok {
			return errors.New("output must be *uint16")
		}
		*ptr = *(*uint16)(unsafe.Pointer(&data[0]))
		return nil
	},

	Uint32Type: func(data []byte, out interface{}) error {
		if len(data) < 4 {
			return errors.New("insufficient data for Uint32 (need 4 bytes)")
		}
		ptr, ok := out.(*uint32)
		if !ok {
			return errors.New("output must be *uint32")
		}
		*ptr = *(*uint32)(unsafe.Pointer(&data[0]))
		return nil
	},

	Uint64Type: func(data []byte, out interface{}) error {
		if len(data) < 8 {
			return errors.New("insufficient data for Uint64 (need 8 bytes)")
		}
		ptr, ok := out.(*uint64)
		if !ok {
			return errors.New("output must be *uint64")
		}
		*ptr = *(*uint64)(unsafe.Pointer(&data[0]))
		return nil
	},

	Float32Type: func(data []byte, out interface{}) error {
		if len(data) < 4 {
			return errors.New("insufficient data for Float32 (need 4 bytes)")
		}
		ptr, ok := out.(*float32)
		if !ok {
			return errors.New("output must be *float32")
		}
		*ptr = *(*float32)(unsafe.Pointer(&data[0]))
		return nil
	},

	Float64Type: func(data []byte, out interface{}) error {
		if len(data) < 8 {
			return errors.New("insufficient data for Float64 (need 8 bytes)")
		}
		ptr, ok := out.(*float64)
		if !ok {
			return errors.New("output must be *float64")
		}
		*ptr = *(*float64)(unsafe.Pointer(&data[0]))
		return nil
	},

	StringType: func(data []byte, out interface{}) error {
		ptr, ok := out.(*string)
		if !ok {
			return errors.New("output must be *string")
		}
		*ptr = strings.TrimRight(string(data), "\x00")
		return nil
	},

	BoolType: func(data []byte, out interface{}) error {
		if len(data) < 1 {
			return errors.New("insufficient data for Bool (need 1 byte)")
		}
		ptr, ok := out.(*bool)
		if !ok {
			return errors.New("output must be *bool")
		}
		*ptr = data[0] != 0
		return nil
	},

	TimestampType: func(data []byte, out interface{}) error {
		if len(data) < 8 {
			return errors.New("insufficient data for Timestamp (need 8 bytes)")
		}
		ptr, ok := out.(*time.Time)
		if !ok {
			return errors.New("output must be *time.Time")
		}
		*ptr = time.Unix(0, *(*int64)(unsafe.Pointer(&data[0])))
		return nil
	},
}

// ReadValue es una función helper para usar los readers de forma segura
func ReadValue(dataType DataType, data []byte, out interface{}) error {
	reader, ok := ReaderTypes[dataType]
	if !ok {
		return errors.New("unsupported data type")
	}
	return reader(data, out)
}
