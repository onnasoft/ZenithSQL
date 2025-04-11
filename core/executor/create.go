package executor

import (
	"context"
	"time"

	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/onnasoft/ZenithSQL/io/response"
	"github.com/onnasoft/ZenithSQL/io/statement"
)

func (e *DefaultExecutor) executeCreateTable(ctx context.Context, stmt *statement.CreateTableStatement) response.Response {
	select {
	case <-ctx.Done():
		return response.NewCreateTableResponse(false, "context canceled")
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
		return response.NewCreateTableResponse(false, err.Error())
	}

	return response.NewCreateTableResponse(true, "table created")
}
