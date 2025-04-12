package buffer

import "github.com/onnasoft/ZenithSQL/model/fields"

type ScanFunc func(value interface{}) (bool, error)

type Scanner struct {
	Type     fields.DataType
	Scan     ScanFunc
	Nullable bool
}
