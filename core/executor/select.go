package executor

import (
	"context"

	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/onnasoft/ZenithSQL/io/response"
	"github.com/onnasoft/ZenithSQL/io/statement"
)

func (e *DefaultExecutor) executeSelect(ctx context.Context, stmt *statement.SelectStatement) response.Response {
	table, err := e.catalog.GetTable(stmt.Database, stmt.Schema, stmt.TableName)
	if err != nil {
		return response.NewSelectResponse(false, err.Error(), nil)
	}

	cursor, err := table.CursorWithFilter(stmt.Where)
	if err != nil {
		return response.NewSelectResponse(false, err.Error(), nil)
	}
	defer cursor.Close()

	cursor.Skip(int64(stmt.Offset))
	if stmt.Limit > 0 {
		cursor.Limit(int64(stmt.Limit))
	}

	if len(stmt.Aggregations) > 0 {
		return e.processAggregations(ctx, stmt, cursor)
	}

	return e.processSimpleSelect(ctx, stmt, cursor)
}

func (e *DefaultExecutor) processSimpleSelect(ctx context.Context, stmt *statement.SelectStatement, cursor storage.Cursor) response.Response {
	rows := []map[string]interface{}{}

	for cursor.Next() {
		select {
		case <-ctx.Done():
			return response.NewSelectResponse(false, "context done", nil)
		default:
		}

		record := make(map[string]interface{})

		for _, column := range stmt.Columns {
			value, err := cursor.ScanField(column)
			if err != nil {
				return response.NewSelectResponse(false, err.Error(), nil)
			}
			record[column] = value
		}

		rows = append(rows, record)
	}

	return response.NewSelectResponse(true, "Select executed successfully", rows)
}

func (e *DefaultExecutor) processAggregations(ctx context.Context, stmt *statement.SelectStatement, cursor storage.Cursor) response.Response {
	// Implementación futura del procesamiento de agregaciones y GROUP BY
	return response.NewSelectResponse(false, "aggregations not implemented yet", nil)
}
