package executor

import (
	"context"

	"github.com/onnasoft/ZenithSQL/io/response"
	"github.com/onnasoft/ZenithSQL/io/statement"
)

func (e *DefaultExecutor) executeDropTable(ctx context.Context, stmt *statement.DropTableStatement) response.Response {
	select {
	case <-ctx.Done():
		return response.NewDropTableResponse(false, "context canceled")
	default:
	}

	dbname := stmt.Database
	schema := stmt.Schema
	tableName := stmt.TableName

	if err := e.catalog.DropTable(dbname, schema, tableName); err != nil {
		return response.NewDropTableResponse(false, err.Error())
	}
	return response.NewDropTableResponse(true, "table dropped")
}
