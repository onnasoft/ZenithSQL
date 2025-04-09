package executor

import (
	"context"

	"github.com/onnasoft/ZenithSQL/io/statement"
)

func (e *DefaultExecutor) executeDropTable(ctx context.Context, stmt *statement.DropTableStatement) (any, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	dbname := stmt.Database
	schema := stmt.Schema
	tableName := stmt.TableName

	if err := e.catalog.DropTable(dbname, schema, tableName); err != nil {
		return nil, err
	}
	return nil, nil
}
