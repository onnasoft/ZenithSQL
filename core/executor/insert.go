package executor

import (
	"context"
	"time"

	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/onnasoft/ZenithSQL/io/response"
	"github.com/onnasoft/ZenithSQL/io/statement"
	"github.com/onnasoft/ZenithSQL/model/catalog"
)

func (e *DefaultExecutor) executeInsert(ctx context.Context, stmt *statement.InsertStatement) response.Response {
	startTime := time.Now()
	table, err := e.catalog.GetTable(stmt.Database, stmt.Schema, stmt.TableName)
	if err != nil {
		return response.NewInsertResponse(false, err.Error(), nil, 0, 0)
	}

	table.LockInsert()
	defer table.UnlockInsert()

	writer, ids, err := insert(ctx, table, stmt.Values...)
	if err != nil {
		return response.NewInsertResponse(false, err.Error(), nil, 0, time.Since(startTime).Milliseconds())
	}
	defer writer.Close()

	if err := table.UpdateRowCount(table.RowCount() + int64(len(stmt.Values))); err != nil {
		writer.Rollback()
		return response.NewInsertResponse(false, err.Error(), nil, 0, time.Since(startTime).Milliseconds())
	}

	if err := writer.Commit(); err != nil {
		writer.Rollback()
		return response.NewInsertResponse(false, err.Error(), nil, 0, time.Since(startTime).Milliseconds())
	}

	return response.NewInsertResponse(
		true,
		"inserted successfully",
		ids,
		int64(len(stmt.Values)),
		time.Since(startTime).Milliseconds(),
	)
}

func insert(ctx context.Context, table *catalog.Table, values ...map[string]interface{}) (storage.Writer, []interface{}, error) {
	now := time.Now()

	writer, err := table.Writer()
	if err != nil {
		return nil, nil, err
	}
	ids := make([]interface{}, 0, len(values))
	id := table.GetNextID()
	for _, row := range values {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
		}

		row["deleted_at"] = nil
		row["created_at"] = now
		row["updated_at"] = now
		row["id"] = id
		ids = append(ids, id)

		if err := writer.Write(row); err != nil {
			return nil, nil, err
		}

		id++
	}

	return writer, ids, nil
}
