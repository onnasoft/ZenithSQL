package storage

import "github.com/onnasoft/ZenithSQL/model/types"

type ColumnData interface {
	types.DataType
	Name() string
	Type() types.DataType
	String() string
}
