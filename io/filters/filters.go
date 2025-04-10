package filters

import (
	"github.com/onnasoft/ZenithSQL/model/types"
)

type filterFn func() (bool, error)
type applyFilter func(f *Filter) (filterFn, error)

var mapEqOps = map[types.DataType]applyFilter{
	types.Int8Type{}:      filterInt8,
	types.Int16Type{}:     filterInt16,
	types.Int32Type{}:     filterInt32,
	types.Int64Type{}:     filterInt64,
	types.Uint8Type{}:     filterUint8,
	types.Uint16Type{}:    filterUint16,
	types.Uint32Type{}:    filterUint32,
	types.Uint64Type{}:    filterUint64,
	types.Float32Type{}:   filterFloat32,
	types.Float64Type{}:   filterFloat64,
	types.StringType{}:    filterString,
	types.BoolType{}:      filterBool,
	types.TimestampType{}: filterTimestamp,
}
