package executor

import (
	"github.com/onnasoft/ZenithSQL/model/catalog"
	"github.com/onnasoft/ZenithSQL/model/record"
)

func Import(table *catalog.Table, e ...*record.Row) error {
	return insert(table, e...)
}
