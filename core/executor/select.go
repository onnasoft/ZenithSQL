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
	cursor, err := table.Cursor()
	if err != nil {
		return response.NewSelectResponse(false, err.Error(), nil)
	}
	defer cursor.Close()

	if stmt.Where != nil {
		cursor, err = cursor.WithFilter(stmt.Where)
		if err != nil {
			return response.NewSelectResponse(false, err.Error(), nil)
		}
	}

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

type aggState struct {
	count int
	sum   float64
	max   *float64
	min   *float64
}

func (e *DefaultExecutor) processAggregations(ctx context.Context, stmt *statement.SelectStatement, cursor storage.Cursor) response.Response {
	groupKeys := stmt.GroupBy

	type groupData struct {
		GroupVals map[string]interface{}
		Aggs      map[string]*aggState
	}

	groups := make(map[string]*groupData)

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

		groupKeyParts := make([]string, len(groupKeys))
		groupVals := make(map[string]interface{}, len(groupKeys))
		for i, key := range groupKeys {
			val := fmt.Sprintf("%v", row[key])
			groupKeyParts[i] = val
			groupVals[key] = row[key]
		}
		groupKey := strings.Join(groupKeyParts, "|")

		if _, ok := groups[groupKey]; !ok {
			groups[groupKey] = &groupData{
				GroupVals: groupVals,
				Aggs:      make(map[string]*aggState),
			}
		}

		for _, agg := range stmt.Aggregations {
			v, ok := toFloat64(row[agg.Column])
			if !ok && agg.Function != "COUNT" {
				continue
			}

			state := groups[groupKey].Aggs[agg.Alias]
			if state == nil {
				state = &aggState{}
				groups[groupKey].Aggs[agg.Alias] = state
			}

			switch agg.Function {
			case "COUNT":
				state.count++
			case "SUM", "AVG":
				state.count++
				state.sum += v
			case "MAX":
				if state.max == nil || v > *state.max {
					state.max = &v
				}
			case "MIN":
				if state.min == nil || v < *state.min {
					state.min = &v
				}
			default:
				return response.NewSelectResponse(false, "unsupported aggregation: "+agg.Function, nil)
			}
		}
	}

	var results []map[string]interface{}

	for _, group := range groups {
		result := make(map[string]interface{})

		for k, v := range group.GroupVals {
			result[k] = v
		}

		for _, agg := range stmt.Aggregations {
			state := group.Aggs[agg.Alias]
			switch agg.Function {
			case "COUNT":
				result[agg.Alias] = state.count
			case "SUM":
				result[agg.Alias] = state.sum
			case "AVG":
				if state.count == 0 {
					result[agg.Alias] = nil
				} else {
					result[agg.Alias] = state.sum / float64(state.count)
				}
			case "MAX":
				if state.max != nil {
					result[agg.Alias] = *state.max
				}
			case "MIN":
				if state.min != nil {
					result[agg.Alias] = *state.min
				}
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
