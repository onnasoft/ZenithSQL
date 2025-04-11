package executor

import (
	"context"

	"github.com/onnasoft/ZenithSQL/io/response"
	"github.com/onnasoft/ZenithSQL/io/statement"
)

func (e *DefaultExecutor) executeTruncateTable(ctx context.Context, stmt *statement.TruncateTableStatement) response.Response {
	table, err := e.catalog.GetTable(stmt.Database, stmt.Schema, stmt.TableName)
	if err != nil {
		return response.NewTruncateTableResponse(false, err.Error())
	}

	select {
	case <-ctx.Done():
		return response.NewTruncateTableResponse(false, "context canceled")
	default:
	}

	if err := table.Truncate(); err != nil {
		return response.NewTruncateTableResponse(false, err.Error())
	}

	return response.NewTruncateTableResponse(true, "table truncated successfully")
}
