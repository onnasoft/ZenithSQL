package executor

import (
	"context"
	"time"

	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/onnasoft/ZenithSQL/io/statement"
)

func (e *DefaultExecutor) executeCreateTable(ctx context.Context, stmt *statement.CreateTableStatement) (any, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	dbname := stmt.Database
	schema := stmt.Schema
	tableName := stmt.TableName
	config := &storage.TableConfig{
		Fields: stmt.FieldsMeta,
		Stats: &storage.StorageStats{
			TotalRows:    0,
			LastModified: time.Now(),
		},
	}

	if _, err := e.catalog.CreateTable(dbname, schema, tableName, config); err != nil {
		return nil, err
	}
	return nil, nil
}
