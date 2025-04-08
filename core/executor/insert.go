package executor

import (
	"context"
	"time"

	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/onnasoft/ZenithSQL/io/statement"
	"github.com/onnasoft/ZenithSQL/model/catalog"
)

func (e *DefaultExecutor) executeInsert(ctx context.Context, stmt *statement.InsertStatement) (any, error) {
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

	if err := table.UpdateRowCount(table.RowCount() + int64(len(stmt.Values))); err != nil {
		writer.Rollback()
		return nil, err
	}

	if err := writer.Commit(); err != nil {
		writer.Rollback()
		return nil, err
	}

	return nil, nil
}

func insert(ctx context.Context, table *catalog.Table, values ...map[string]interface{}) (storage.Writer, error) {
	now := time.Now()

	writer, err := table.Writer()
	if err != nil {
		return nil, err
	}

	id := table.GetNextID()
	for _, row := range values {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		row["created_at"] = now
		row["updated_at"] = now
		row["id"] = id

		if err := writer.Write(row); err != nil {
			return nil, err
		}

		id++
	}

	return writer, nil
}
