package filters

import "github.com/onnasoft/ZenithSQL/model/fields"

type filterFn func() (bool, error)
type applyFilter func(f *Filter) (filterFn, error)

var mapEqOps = map[fields.DataType]applyFilter{
	fields.Int8Type{}:      filterInt8,
	fields.Int16Type{}:     filterInt16,
	fields.Int32Type{}:     filterInt32,
	fields.Int64Type{}:     filterInt64,
	fields.Uint8Type{}:     filterUint8,
	fields.Uint16Type{}:    filterUint16,
	fields.Uint32Type{}:    filterUint32,
	fields.Uint64Type{}:    filterUint64,
	fields.Float32Type{}:   filterFloat32,
	fields.Float64Type{}:   filterFloat64,
	fields.StringType{}:    filterString,
	fields.BoolType{}:      filterBool,
	fields.TimestampType{}: filterTimestamp,
}
