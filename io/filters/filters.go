package filters

import (
	"github.com/onnasoft/ZenithSQL/model/types"
)

type filterFn func() (bool, error)
type applyFilter func(f *Filter) (filterFn, error)

var mapEqOps = map[types.DataType]applyFilter{
	types.Int8Type{}: filterInt8,
}
