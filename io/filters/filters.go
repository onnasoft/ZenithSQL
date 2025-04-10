package filters

import (
	"github.com/onnasoft/ZenithSQL/model/types"
)

type filterFn func() (bool, error)
type applyFilter func(f *Filter) (filterFn, error)

var mapEqOps = map[types.Types]applyFilter{
	types.Int8: filterInt8,
}
