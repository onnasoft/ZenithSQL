package executor

import (
	"context"

	"github.com/onnasoft/ZenithSQL/io/statement"
)

func (e *DefaultExecutor) executeTruncateTable(ctx context.Context, stmt *statement.TruncateTableStatement) (any, error) {
	table, err := e.catalog.GetTable(stmt.Database, stmt.Schema, stmt.TableName)
	if err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if err := table.Truncate(); err != nil {
		return nil, err
	}

	return nil, nil
}
