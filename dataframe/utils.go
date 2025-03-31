package dataframe

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
