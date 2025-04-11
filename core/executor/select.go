package executor

import (
	"context"

	"github.com/onnasoft/ZenithSQL/io/filters"
	"github.com/onnasoft/ZenithSQL/io/response"
	"github.com/onnasoft/ZenithSQL/io/statement"
)

func (e *DefaultExecutor) executeSelect(ctx context.Context, stmt *statement.SelectStatement) response.Response {
	table, err := e.catalog.GetTable(stmt.Database, stmt.Schema, stmt.TableName)
	if err != nil {
		return response.NewSelectResponse(false, err.Error(), nil)
	}
	filter := filters.NewCondition("age", filters.Equal, int8(12))

	cursor, err := table.CursorWithFilter(filter)
	if err != nil {
		return response.NewSelectResponse(false, err.Error(), nil)
	}
	defer cursor.Close()

	rows := []map[string]interface{}{}
	for cursor.Next() {
		record := map[string]interface{}{}
		err := cursor.Scan(record)
		if err != nil {
			return response.NewSelectResponse(false, err.Error(), nil)
		}
	}

	return response.NewSelectResponse(
		true,
		"Select executed successfully",
		rows,
	)
}
