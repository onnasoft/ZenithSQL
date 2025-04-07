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

func (dt DataType) Reader() (ReaderFunc, error) {
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
