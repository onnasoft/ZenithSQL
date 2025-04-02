package entity

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"
)

var writerTypes = map[DataType]func(buffer []byte, field *Field, val interface{}) error{
	Int8Type: func(buffer []byte, field *Field, value interface{}) error {
		v, ok := value.(int8)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Int8 field %s", field.Name)
		}
		buffer[field.StartPosition] = uint8(v)
		return nil
	},
	Int16Type: func(buffer []byte, field *Field, value interface{}) error {
		v, ok := value.(int16)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Int16 field %s", field.Name)
		}
		binary.LittleEndian.PutUint16(buffer[field.StartPosition:], uint16(v))
		return nil
	},
	Int32Type: func(buffer []byte, field *Field, value interface{}) error {
		v, ok := value.(int32)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Int32 field %s", field.Name)
		}
		binary.LittleEndian.PutUint32(buffer[field.StartPosition:], uint32(v))
		return nil
	},
	Int64Type: func(buffer []byte, field *Field, value interface{}) error {
		v, ok := value.(int64)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Int64 field %s", field.Name)
		}
		binary.LittleEndian.PutUint64(buffer, uint64(v))
		return nil
	},
	Uint8Type: func(buffer []byte, field *Field, value interface{}) error {
		v, ok := value.(uint8)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Uint8 field %s", field.Name)
		}
		buffer[field.StartPosition] = v
		return nil
	},
	Uint16Type: func(buffer []byte, field *Field, value interface{}) error {
		v, ok := value.(uint16)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Uint16 field %s", field.Name)
		}
		binary.LittleEndian.PutUint16(buffer[field.StartPosition:], v)
		return nil
	},
	Uint32Type: func(buffer []byte, field *Field, value interface{}) error {
		v, ok := value.(uint32)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Uint32 field %s", field.Name)
		}
		binary.LittleEndian.PutUint32(buffer[field.StartPosition:], v)
		return nil
	},
	Uint64Type: func(buffer []byte, field *Field, value interface{}) error {
		v, ok := value.(uint64)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Uint64 field %s", field.Name)
		}
		binary.LittleEndian.PutUint64(buffer[field.StartPosition:], v)
		return nil
	},
	Float32Type: func(buffer []byte, field *Field, value interface{}) error {
		v, ok := value.(float32)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Float32 field %s", field.Name)
		}
		binary.LittleEndian.PutUint32(buffer[field.StartPosition:], math.Float32bits(v))
		return nil
	},
	Float64Type: func(buffer []byte, field *Field, value interface{}) error {
		v, ok := value.(float64)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Float64 field %s", field.Name)
		}
		binary.LittleEndian.PutUint64(buffer, math.Float64bits(v))
		return nil
	},
	StringType: func(buffer []byte, field *Field, value interface{}) error {
		v, ok := value.(string)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for String field %s", field.Name)
		}
		copy(buffer, v)
		// Rellenar con ceros si es necesario
		if len(v) < field.Length {
			clear(buffer[len(v):])
		}
		return nil
	},
	TimestampType: func(buffer []byte, field *Field, value interface{}) error {
		v, ok := value.(time.Time)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Timestamp field %s", field.Name)
		}
		binary.LittleEndian.PutUint64(buffer, uint64(v.UnixNano()))
		return nil
	},
	BoolType: func(buffer []byte, field *Field, value interface{}) error {
		v, ok := value.(bool)
		if !ok && value != nil {
			return fmt.Errorf("type assertion failed for Bool field %s", field.Name)
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
