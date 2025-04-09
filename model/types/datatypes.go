package types

type DataType interface {
	ResolveLength(length int) (int, error)
	Read([]byte, interface{}) error
	Write([]byte, interface{}) error
	Valid(interface{}) error
	Parse([]byte) interface{}
	String() string
}

type Types string

const (
	Int8      Types = "int8"
	Int16     Types = "int16"
	Int32     Types = "int32"
	Int64     Types = "int64"
	Uint8     Types = "uint8"
	Uint16    Types = "uint16"
	Uint32    Types = "uint32"
	Uint64    Types = "uint64"
	Float32   Types = "float32"
	Float64   Types = "float64"
	String    Types = "string"
	Bool      Types = "bool"
	Timestamp Types = "timestamp"
	Unknown   Types = "unknown"
)

var mapTypes = map[Types]DataType{
	"int8":      Int8Type{},
	"int16":     Int16Type{},
	"int32":     Int32Type{},
	"int64":     Int64Type{},
	"uint8":     Uint8Type{},
	"uint16":    Uint16Type{},
	"uint32":    Uint32Type{},
	"uint64":    Uint64Type{},
	"float32":   Float32Type{},
	"float64":   Float64Type{},
	"string":    StringType{},
	"bool":      BoolType{},
	"timestamp": TimestampType{},
	"unknown":   UnknownType{},
}

func NewDataType(dt Types) DataType {
	dataType, ok := mapTypes[dt]
	if !ok {
		dataType = UnknownType{}
	}

	return dataType
}
