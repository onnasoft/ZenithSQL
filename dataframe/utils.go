package dataframe

import "github.com/onnasoft/ZenithSQL/entity"

func isValidType(dt entity.DataType, val interface{}) bool {
	switch dt {
	case entity.Int64Type, entity.TimestampType:
		_, ok := val.(int64)
		return ok
	case entity.Float64Type:
		_, ok := val.(float64)
		return ok
	case entity.StringType:
		_, ok := val.(string)
		return ok
	case entity.BoolType:
		_, ok := val.(bool)
		return ok
	default:
		return false
	}
}
