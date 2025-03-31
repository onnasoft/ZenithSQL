package entity

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"
)

var writerTypes = map[DataType]func(buffer []byte, field *Field, val interface{}) error{
	Int8Type: func(buffer []byte, field *Field, val interface{}) error {
		buffer[field.StartPosition] = uint8(val.(int64))
		return nil
	},
	Int16Type: func(buffer []byte, field *Field, val interface{}) error {
		binary.LittleEndian.PutUint16(buffer[field.StartPosition:], uint16(val.(int64)))
		return nil
	},
	Int32Type: func(buffer []byte, field *Field, val interface{}) error {
		binary.LittleEndian.PutUint32(buffer[field.StartPosition:], uint32(val.(int64)))
		return nil
	},
	Int64Type: func(buffer []byte, field *Field, val interface{}) error {
		binary.LittleEndian.PutUint64(buffer[field.StartPosition:], uint64(val.(int64)))
		return nil
	},
	Uint8Type: func(buffer []byte, field *Field, val interface{}) error {
		buffer[field.StartPosition] = uint8(val.(int64))
		return nil
	},
	Uint16Type: func(buffer []byte, field *Field, val interface{}) error {
		binary.LittleEndian.PutUint16(buffer[field.StartPosition:], uint16(val.(int64)))
		return nil
	},
	Uint32Type: func(buffer []byte, field *Field, val interface{}) error {
		binary.LittleEndian.PutUint32(buffer[field.StartPosition:], uint32(val.(int64)))
		return nil
	},
	Uint64Type: func(buffer []byte, field *Field, val interface{}) error {
		binary.LittleEndian.PutUint64(buffer[field.StartPosition:], uint64(val.(int64)))
		return nil
	},
	Float32Type: func(buffer []byte, field *Field, val interface{}) error {
		binary.LittleEndian.PutUint32(buffer[field.StartPosition:], math.Float32bits(val.(float32)))
		return nil
	},
	Float64Type: func(buffer []byte, field *Field, val interface{}) error {
		binary.LittleEndian.PutUint64(buffer[field.StartPosition:], math.Float64bits(val.(float64)))
		return nil
	},
	StringType: func(buffer []byte, field *Field, val interface{}) error {
		str := val.(string)
		if len(str) > field.Length {
			return fmt.Errorf("string length exceeds maximum length of %d", field.Length)
		}
		copy(buffer[field.StartPosition:], str)
		for j := len(str); j < field.Length; j++ {
			buffer[field.StartPosition+j] = 0
		}
		return nil
	},
	TimestampType: func(buffer []byte, field *Field, val interface{}) error {
		binary.LittleEndian.PutUint64(buffer[field.StartPosition:], uint64(val.(time.Time).UnixNano()))
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
