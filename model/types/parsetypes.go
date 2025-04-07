package types

import (
	"strings"
	"time"
	"unsafe"
)

var ParseTypes = map[DataType]func([]byte) interface{}{
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
