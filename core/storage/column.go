package storage

import (
	"github.com/onnasoft/ZenithSQL/model/fields"
)

type ColumnData interface {
	fields.DataType
	Name() string
	Type() fields.DataType
	String() string
}
