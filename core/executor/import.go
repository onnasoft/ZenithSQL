package executor

import (
	"context"

	"github.com/onnasoft/ZenithSQL/io/statement"
	"github.com/onnasoft/ZenithSQL/model/catalog"
)

func (e *DefaultExecutor) executeImport(ctx context.Context, stmt *statement.ImportStatement) (any, error) {
	table, err := e.catalog.GetTable(stmt.Database, stmt.Schema, stmt.TableName)
	if err != nil {
		return nil, err
	}

	table.LockInsert()
	defer table.UnlockInsert()

	writer, err := insert(ctx, table, stmt.Values...)
	if err != nil {
		return nil, err
	}
	defer writer.Close()

	if err := writer.Commit(); err != nil {
		writer.Rollback()
		return nil, err
	}

	if err := table.UpdateRowCount(table.RowCount() + int64(len(stmt.Values))); err != nil {
		writer.Rollback()
		return nil, err
	}

	return nil, nil
}

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
