package entity

import (
	"fmt"
	"strings"
	"time"
	"unsafe"

	"github.com/onnasoft/ZenithSQL/core/buffer"
)

var writerTypes = map[DataType]func(buffer []byte, val interface{}) error{
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

var parseTypes = map[DataType]func([]byte) interface{}{
	Int8Type: func(b []byte) interface{} {
		return int8(b[0])
	},
	Int16Type: func(b []byte) interface{} {
		return *(*int16)(unsafe.Pointer(&b[0]))
	},
	Int32Type: func(b []byte) interface{} {
		return *(*int32)(unsafe.Pointer(&b[0]))
	},
	Int64Type: func(b []byte) interface{} {
		return *(*int64)(unsafe.Pointer(&b[0]))
	},
	Uint8Type: func(b []byte) interface{} {
		return uint8(b[0])
	},
	Uint16Type: func(b []byte) interface{} {
		return *(*uint16)(unsafe.Pointer(&b[0]))
	},
	Uint32Type: func(b []byte) interface{} {
		return *(*uint32)(unsafe.Pointer(&b[0]))
	},
	Uint64Type: func(b []byte) interface{} {
		return *(*uint64)(unsafe.Pointer(&b[0]))
	},
	Float32Type: func(b []byte) interface{} {
		return *(*float32)(unsafe.Pointer(&b[0]))
	},
	Float64Type: func(b []byte) interface{} {
		return *(*float64)(unsafe.Pointer(&b[0]))
	},
	StringType: func(b []byte) interface{} {
		return strings.TrimRight(string(b), "\x00")
	},
	TimestampType: func(b []byte) interface{} {
		return time.Unix(0, *(*int64)(unsafe.Pointer(&b[0])))
	},
}
var typeValidators = map[DataType]func(interface{}) bool{
	Int8Type: func(val interface{}) bool {
		_, ok := val.(int8)
		return ok
	},
	Int16Type: func(val interface{}) bool {
		_, ok := val.(int16)
		return ok
	},
	Int32Type: func(val interface{}) bool {
		_, ok := val.(int32)
		return ok
	},
	Int64Type: func(val interface{}) bool {
		_, ok := val.(int64)
		return ok
	},
	Uint8Type: func(val interface{}) bool {
		_, ok := val.(uint8)
		return ok
	},
	Uint16Type: func(val interface{}) bool {
		_, ok := val.(uint16)
		return ok
	},
	Uint32Type: func(val interface{}) bool {
		_, ok := val.(uint32)
		return ok
	},
	Uint64Type: func(val interface{}) bool {
		_, ok := val.(uint64)
		return ok
	},
	Float32Type: func(val interface{}) bool {
		_, ok := val.(float32)
		return ok
	},
	Float64Type: func(val interface{}) bool {
		_, ok := val.(float64)
		return ok
	},
	StringType: func(val interface{}) bool {
		_, ok := val.(string)
		return ok
	},
	BoolType: func(val interface{}) bool {
		_, ok := val.(bool)
		return ok
	},
	TimestampType: func(val interface{}) bool {
		if val == nil {
			return true
		}
		_, ok := val.(time.Time)
		if !ok {
			_, ok = val.(int64)
		}
		return ok
	},
}

func isValidType(dt DataType, val interface{}) bool {
	if validator, exists := typeValidators[dt]; exists {
		return validator(val)
	}
	return false
}

func decodeField(field *Field, data []byte) (interface{}, error) {
	if parser, ok := parseTypes[field.Type]; ok {
		return parser(data), nil
	}
	return nil, fmt.Errorf("unsupported field type: %s", field.Type)
}

func encodeField(field *Field, value interface{}, buffer []byte) error {
	if field.Length <= 0 {
		return fmt.Errorf("invalid field length %d for %s", field.Length, field.Name)
	}
	if len(buffer) < field.Length {
		return fmt.Errorf("buffer too small for field %s (need %d, got %d, start: %d, end: %d)",
			field.Name, field.Length, len(buffer), field.StartPosition, field.EndPosition)
	}
	if value == nil {
		return nil
	}

	if writer, ok := writerTypes[field.Type]; ok {
		return writer(buffer, value)
	}
	return fmt.Errorf("unsupported field type: %s", field.Type)
}

func GetValue(f *Field, rw buffer.ReadWriter) interface{} {
	isSet := make([]byte, 1)
	rw.ReadAt(isSet, f.IsSettedFlagPos)
	if isSet[0] == 0 {
		return nil
	}

	data := make([]byte, f.Length)
	rw.ReadAt(data, f.StartPosition)
	value, _ := decodeField(f, data)

	return value
}

func GetFloat64ValueAtOffset(field *Field, buff *buffer.Buffer, offset int) float64 {
	isSet, _ := buff.Read(field.IsSettedFlagPos, 1)
	if isSet[0] == 0 {
		return 0
	}
	data, _ := buff.Read(offset+field.StartPosition, field.Length)
	return *(*float64)(unsafe.Pointer(&data[0]))
}
