package types

import "fmt"

type DataType string

const (
	Int8Type  DataType = "int8"
	Int16Type DataType = "int16"
	Int32Type DataType = "int32"
	Int64Type DataType = "int64"

	Uint8Type  DataType = "uint8"
	Uint16Type DataType = "uint16"
	Uint32Type DataType = "uint32"
	Uint64Type DataType = "uint64"

	Float32Type DataType = "float32"
	Float64Type DataType = "float64"

	StringType    DataType = "string"
	BoolType      DataType = "bool"
	TimestampType DataType = "timestamp"

	Unknown = "unknown"
)

func (dt DataType) ResolveLength(length int) (int, error) {
	switch dt {
	case StringType:
		if length > 0 {
			return length, nil
		}
		return 255, nil
	case Int8Type, Uint8Type, BoolType:
		return 1, nil
	case Int16Type, Uint16Type:
		return 2, nil
	case Int32Type, Uint32Type, Float32Type:
		return 4, nil
	case Int64Type, Uint64Type, Float64Type, TimestampType:
		return 8, nil
	default:
		return 0, fmt.Errorf("unsupported or unknown data type: %s", dt)
	}
}

func (dt DataType) Reader() (func([]byte, interface{}) error, error) {
	fn, ok := ReaderTypes[dt]
	if !ok {
		return nil, fmt.Errorf("unknown data type: %s", dt)
	}

	return fn, nil
}

func (dt DataType) Writer() (func(buffer []byte, val interface{}) error, error) {
	fn, ok := WriterTypes[dt]
	if !ok {
		return nil, fmt.Errorf("unknown data type: %s", dt)
	}

	return fn, nil
}

func (dt DataType) Valid() func(val interface{}) error {
	fn, ok := ValidatorTypes[dt]
	if !ok {
		return nil
	}

	return fn
}

func (dt DataType) Parser() (func([]byte) interface{}, error) {
	fn, ok := ParseTypes[dt]
	if !ok {
		return nil, fmt.Errorf("unknown data type: %s", dt)
	}

	return fn, nil
}
