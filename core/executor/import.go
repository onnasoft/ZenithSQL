package executor

import (
	"context"
	"time"

	"github.com/onnasoft/ZenithSQL/io/response"
	"github.com/onnasoft/ZenithSQL/io/statement"
)

func (e *DefaultExecutor) executeImport(ctx context.Context, stmt *statement.ImportStatement) response.Response {
	startTime := time.Now()

	table, err := e.catalog.GetTable(stmt.Database, stmt.Schema, stmt.TableName)
	if err != nil {
		return response.NewImportResponse(false, err.Error(), 0, time.Since(startTime).Milliseconds())
	}

	table.LockInsert()
	defer table.UnlockInsert()

	writer, _, err := insert(ctx, table, stmt.Values...)
	if err != nil {
		return response.NewImportResponse(false, err.Error(), 0, time.Since(startTime).Milliseconds())
	}
	defer writer.Close()

	if err := writer.Commit(); err != nil {
		writer.Rollback()
		return response.NewImportResponse(false, err.Error(), 0, time.Since(startTime).Milliseconds())
	}

	if err := table.UpdateRowCount(table.RowCount() + int64(len(stmt.Values))); err != nil {
		writer.Rollback()
		return response.NewImportResponse(false, err.Error(), 0, time.Since(startTime).Milliseconds())
	}

	return response.NewImportResponse(
		true,
		"imported successfully",
		int64(len(stmt.Values)),
		time.Since(startTime).Milliseconds(),
	)
}
