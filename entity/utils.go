package entity

import (
	"encoding/binary"
	"math"
	"strings"
	"time"
)

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
