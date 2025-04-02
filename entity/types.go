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

	Unknown = 100
)

func DataTypeFromString(value string) DataType {
	switch value {
	case "int8":
		return Int8Type
	case "int16":
		return Int16Type
	case "int32":
		return Int32Type
	case "int64":
		return Int64Type
	case "uint8":
		return Uint8Type
	case "uint16":
		return Uint16Type
	case "uint32":
		return Uint32Type
	case "uint64":
		return Uint64Type
	case "float32":
		return Float32Type
	case "float64":
		return Float64Type
	case "string":
		return StringType
	case "bool":
		return BoolType
	case "timestamp":
		return TimestampType
	}

	return Unknown
}

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
