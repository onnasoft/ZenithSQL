package executor

import (
	"context"

	"github.com/onnasoft/ZenithSQL/model/catalog"
)

func Import(table *catalog.Table, values ...map[string]interface{}) error {
	writer, err := insert(context.Background(), table, values...)
	if err != nil {
		return err
	}
	defer writer.Close()

	if err := writer.Commit(); err != nil {
		writer.Rollback()
		return err
	}

	if err := table.UpdateRowCount(table.RowCount() + int64(len(values))); err != nil {
		writer.Rollback()
		return err
	}

	return nil
}
