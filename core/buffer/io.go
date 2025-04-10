package buffer

import "github.com/onnasoft/ZenithSQL/model/types"

type ScanFunc func(value interface{}) (bool, error)

type Scanner struct {
	Type     types.DataType
	Scan     ScanFunc
	Nullable bool
}
