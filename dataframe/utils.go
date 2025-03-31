package dataframe

import (
	"fmt"
	"time"
)

var printFuncs = map[DataType]func(interface{}) string{
	Int64Type: func(v interface{}) string {
		return fmt.Sprintf("%d", v.(int64))
	},
	Float64Type: func(v interface{}) string {
		return fmt.Sprintf("%.2f", v.(float64))
	},
	StringType: func(v interface{}) string {
		return fmt.Sprintf("%s", v)
	},
	BoolType: func(v interface{}) string {
		return fmt.Sprintf("%t", v.(bool))
	},
	TimestampType: func(v interface{}) string {
		timestamp := time.Unix(0, v.(int64))
		return timestamp.Format(time.RFC3339)
	},
}

func isValidType(dt DataType, val interface{}) bool {
	switch dt {
	case Int64Type, TimestampType:
		_, ok := val.(int64)
		return ok
	case Float64Type:
		_, ok := val.(float64)
		return ok
	case StringType:
		_, ok := val.(string)
		return ok
	case BoolType:
		_, ok := val.(bool)
		return ok
	default:
		return false
	}
}
