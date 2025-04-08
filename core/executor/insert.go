package executor

import (
	"context"
	"time"

	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/onnasoft/ZenithSQL/io/statement"
	"github.com/onnasoft/ZenithSQL/model/catalog"
)

func (e *DefaultExecutor) executeInsert(ctx context.Context, stmt *statement.InsertStatement) (any, error) {
	database, err := e.catalog.GetDatabase(stmt.Database)
	if err != nil {
		return nil, err
	}

	schema, err := database.GetSchema(stmt.Schema)
	if err != nil {
		return nil, err
	}

	table, err := schema.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	if err := Insert(ctx, table, stmt.Values...); err != nil {
		return nil, err
	}

	return nil, nil
}

func Insert(ctx context.Context, table *catalog.Table, values ...map[string]interface{}) error {
	table.LockInsert()
	defer table.UnlockInsert()

	writer, err := insert(ctx, table, values...)
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
