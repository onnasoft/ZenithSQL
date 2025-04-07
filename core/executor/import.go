package executor

import (
	"github.com/onnasoft/ZenithSQL/model/catalog"
)

func Import(table *catalog.Table, values ...map[string]interface{}) error {
	_, err := insert(table, values...)

	return err
}
