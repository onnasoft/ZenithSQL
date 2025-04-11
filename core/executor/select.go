package executor

import (
	"context"
	"fmt"
	"strings"

	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/onnasoft/ZenithSQL/io/response"
	"github.com/onnasoft/ZenithSQL/io/statement"
)

func (e *DefaultExecutor) executeSelect(ctx context.Context, stmt *statement.SelectStatement) response.Response {
	table, err := e.catalog.GetTable(stmt.Database, stmt.Schema, stmt.TableName)
	if err != nil {
		return response.NewSelectResponse(false, err.Error(), nil)
	}
	var cursor storage.Cursor
	if stmt.Where == nil {
		cursor, err = table.Cursor()
	} else {
		cursor, err = table.CursorWithFilter(stmt.Where)
	}
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
	groupMap := make(map[string][]map[string]interface{})
	groupKeys := stmt.GroupBy

	for cursor.Next() {
		select {
		case <-ctx.Done():
			return response.NewSelectResponse(false, "context done", nil)
		default:
		}

		row := make(map[string]interface{})
		if err := cursor.Scan(row); err != nil {
			return response.NewSelectResponse(false, err.Error(), nil)
		}

		groupKey := ""
		for _, key := range groupKeys {
			groupKey += fmt.Sprintf("%v|", row[key])
		}
		groupMap[groupKey] = append(groupMap[groupKey], row)
	}

	var results []map[string]interface{}

	for key, rows := range groupMap {
		result := make(map[string]interface{})

		// Set grouped fields
		if len(groupKeys) > 0 {
			values := strings.Split(key, "|")
			for i, k := range groupKeys {
				if i < len(values) {
					result[k] = values[i]
				}
			}
		}

		// Compute aggregations
		for _, agg := range stmt.Aggregations {
			switch agg.Function {
			case "COUNT":
				result[agg.Alias] = len(rows)
			case "SUM", "AVG", "MAX", "MIN":
				val := computeNumericAgg(rows, agg.Column, agg.Function)
				result[agg.Alias] = val
			default:
				return response.NewSelectResponse(false, "unsupported aggregation: "+agg.Function, nil)
			}
		}

		results = append(results, result)
	}

	return response.NewSelectResponse(true, "Aggregated select executed successfully", results)
}

func computeNumericAgg(rows []map[string]interface{}, column, fn string) interface{} {
	var sum float64
	var max, min *float64

	for _, row := range rows {
		v, ok := toFloat64(row[column])
		if !ok {
			continue
		}
		sum += v
		if max == nil || v > *max {
			max = &v
		}
		if min == nil || v < *min {
			min = &v
		}
	}

	switch fn {
	case "SUM":
		return sum
	case "AVG":
		if len(rows) == 0 {
			return nil
		}
		return sum / float64(len(rows))
	case "MAX":
		if max != nil {
			return *max
		}
	case "MIN":
		if min != nil {
			return *min
		}
	}
	return nil
}

func toFloat64(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}
