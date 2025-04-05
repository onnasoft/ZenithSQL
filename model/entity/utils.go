package entity

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"
)

var writerTypes = map[DataType]func(buffer []byte, val interface{}) error{
	Int8Type: func(buffer []byte, value interface{}) error {
		v, ok := value.(int8)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Int8")
		}
		buffer[0] = uint8(v)
		return nil
	},
	Int16Type: func(buffer []byte, value interface{}) error {
		v, ok := value.(int16)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Int16")
		}
		binary.LittleEndian.PutUint16(buffer, uint16(v))
		return nil
	},
	Int32Type: func(buffer []byte, value interface{}) error {
		v, ok := value.(int32)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Int32")
		}
		binary.LittleEndian.PutUint32(buffer, uint32(v))
		return nil
	},
	Int64Type: func(buffer []byte, value interface{}) error {
		v, ok := value.(int64)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Int64")
		}
		binary.LittleEndian.PutUint64(buffer, uint64(v))
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
		binary.LittleEndian.PutUint16(buffer, v)
		return nil
	},
	Uint32Type: func(buffer []byte, value interface{}) error {
		v, ok := value.(uint32)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Uint32")
		}
		binary.LittleEndian.PutUint32(buffer, v)
		return nil
	},
	Uint64Type: func(buffer []byte, value interface{}) error {
		v, ok := value.(uint64)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Uint64")
		}
		binary.LittleEndian.PutUint64(buffer, v)
		return nil
	},
	Float32Type: func(buffer []byte, value interface{}) error {
		v, ok := value.(float32)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Float32")
		}
		binary.LittleEndian.PutUint32(buffer, math.Float32bits(v))
		return nil
	},
	Float64Type: func(buffer []byte, value interface{}) error {
		v, ok := value.(float64)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Float64")
		}
		binary.LittleEndian.PutUint64(buffer, math.Float64bits(v))
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
		binary.LittleEndian.PutUint64(buffer, uint64(v.UnixNano()))
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
		return int16(binary.LittleEndian.Uint16(b))
	},
	Int32Type: func(b []byte) interface{} {
		return int32(binary.LittleEndian.Uint32(b))
	},
	Int64Type: func(b []byte) interface{} {
		return int64(binary.LittleEndian.Uint64(b))
	},
	Uint8Type: func(b []byte) interface{} {
		return uint8(b[0])
	},
	Uint16Type: func(b []byte) interface{} {
		return uint16(binary.LittleEndian.Uint16(b))
	},
	Uint32Type: func(b []byte) interface{} {
		return uint32(binary.LittleEndian.Uint32(b))
	},
	Uint64Type: func(b []byte) interface{} {
		fmt.Println("Uint64Type Data:", b)
		return uint64(binary.LittleEndian.Uint64(b))
	},
	Float32Type: func(b []byte) interface{} {
		return math.Float32frombits(binary.LittleEndian.Uint32(b))
	},
	Float64Type: func(b []byte) interface{} {
		return math.Float64frombits(binary.LittleEndian.Uint64(b))
	},
	StringType: func(b []byte) interface{} {
		return strings.TrimRight(string(b), "\x00")
	},
	TimestampType: func(b []byte) interface{} {
		return time.Unix(0, int64(binary.LittleEndian.Uint64(b)))
	},
}

func isValidType(dt DataType, val interface{}) bool {
	switch dt {
	case Int64Type:
		_, ok := val.(int64)
		return ok
	case Float64Type:
		_, ok := val.(float64)
		return ok
	case StringType:
		_, ok := val.(string)
		return ok
	case BoolType:
		_, ok := val.(bool)
		return ok
	case TimestampType:
		if val == nil {
			return true
		}
		_, ok := val.(time.Time)
		if !ok {
			_, ok = val.(int64)
		}
		return ok
	default:
		return false
	}
}
func decodeField(field *Field, data []byte) (interface{}, error) {
	fmt.Println("decodeField Data:", data)
	if parser, ok := parseTypes[field.Type]; ok {
		fmt.Println("decodeField Parser exists for type:", field.Type)
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
