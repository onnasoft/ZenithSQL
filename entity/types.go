package entity

type DataType int

const (
	Int8Type DataType = iota
	Int16Type
	Int32Type
	Int64Type

	Uint8Type
	Uint16Type
	Uint32Type
	Uint64Type

	Float32Type
	Float64Type

	StringType
	BoolType
	TimestampType
)

func (dt DataType) String() string {
	switch dt {
	case Int8Type:
		return "int8"
	case Int16Type:
		return "int16"
	case Int32Type:
		return "int32"
	case Int64Type:
		return "int64"
	case Uint8Type:
		return "uint8"
	case Uint16Type:
		return "uint16"
	case Uint32Type:
		return "uint32"
	case Uint64Type:
		return "uint64"
	case Float32Type:
		return "float32"
	case Float64Type:
		return "float64"
	case StringType:
		return "string"
	case BoolType:
		return "bool"
	case TimestampType:
		return "timestamp"
	default:
		return "unknown"
	}
}
