package types

import (
	"fmt"
	"time"
	"unsafe"
)

var WriterTypes = map[DataType]func(buffer []byte, val interface{}) error{
	Int8Type: func(buffer []byte, value interface{}) error {
		v, ok := value.(int8)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Int8")
		}
		buffer[0] = byte(v)
		return nil
	},
	Int16Type: func(buffer []byte, value interface{}) error {
		v, ok := value.(int16)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Int16")
		}
		*(*int16)(unsafe.Pointer(&buffer[0])) = v
		return nil
	},
	Int32Type: func(buffer []byte, value interface{}) error {
		v, ok := value.(int32)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Int32")
		}
		*(*int32)(unsafe.Pointer(&buffer[0])) = v
		return nil
	},
	Int64Type: func(buffer []byte, value interface{}) error {
		v, ok := value.(int64)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Int64")
		}
		*(*int64)(unsafe.Pointer(&buffer[0])) = v
		return nil
	},
	Uint8Type: func(buffer []byte, value interface{}) error {
		v, ok := value.(uint8)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Uint8")
		}
		buffer[0] = v
		return nil
	},
	Uint16Type: func(buffer []byte, value interface{}) error {
		v, ok := value.(uint16)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Uint16")
		}
		*(*uint16)(unsafe.Pointer(&buffer[0])) = v
		return nil
	},
	Uint32Type: func(buffer []byte, value interface{}) error {
		v, ok := value.(uint32)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Uint32")
		}
		*(*uint32)(unsafe.Pointer(&buffer[0])) = v
		return nil
	},
	Uint64Type: func(buffer []byte, value interface{}) error {
		v, ok := value.(uint64)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Uint64")
		}
		*(*uint64)(unsafe.Pointer(&buffer[0])) = v
		return nil
	},
	Float32Type: func(buffer []byte, value interface{}) error {
		v, ok := value.(float32)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Float32")
		}
		*(*float32)(unsafe.Pointer(&buffer[0])) = v
		return nil
	},
	Float64Type: func(buffer []byte, value interface{}) error {
		v, ok := value.(float64)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Float64")
		}
		*(*float64)(unsafe.Pointer(&buffer[0])) = v
		return nil
	},
	StringType: func(buffer []byte, value interface{}) error {
		v, ok := value.(string)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for String")
		}
		copy(buffer, v)
		if len(v) < len(buffer) {
			clear(buffer[len(v):])
		}
		return nil
	},
	TimestampType: func(buffer []byte, value interface{}) error {
		v, ok := value.(time.Time)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Timestamp")
		}
		*(*int64)(unsafe.Pointer(&buffer[0])) = v.UnixNano()
		return nil
	},
	BoolType: func(buffer []byte, value interface{}) error {
		v, ok := value.(bool)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Bool")
		}
		if v {
			buffer[0] = 1
		} else {
			buffer[0] = 0
		}
		return nil
	},
}
