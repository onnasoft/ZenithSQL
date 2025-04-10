package filters

import (
	"fmt"
	"slices"
)

const errorUnsupportedOperatorInt64 = "unsupported operator %s for type int64"

func filterInt64(f *Filter) (filterFn, error) {
	switch f.Operator {
	case Equal:
		return compareInt64(f, func(a, b int64) bool { return a == b })
	case NotEqual:
		return compareInt64(f, func(a, b int64) bool { return a != b })
	case GreaterThan:
		return compareInt64(f, func(a, b int64) bool { return a > b })
	case GreaterThanOrEqual:
		return compareInt64(f, func(a, b int64) bool { return a >= b })
	case LessThan:
		return compareInt64(f, func(a, b int64) bool { return a < b })
	case LessThanOrEqual:
		return compareInt64(f, func(a, b int64) bool { return a <= b })
	case Like, NotLike:
		return unsupportedLikeInt64(f.Operator)
	case In:
		return containsInt64(f, true)
	case NotIn:
		return containsInt64(f, false)
	case IsNull:
		return isNullInt64(f, true)
	case IsNotNull:
		return isNullInt64(f, false)
	case Between:
		return betweenInt64(f, true)
	case NotBetween:
		return betweenInt64(f, false)
	default:
		return nil, fmt.Errorf(errorUnsupportedOperatorInt64, f.Operator)
	}
}

func compareInt64(f *Filter, cmp func(a, b int64) bool) (filterFn, error) {
	data, ok := f.Value.(int64)
	if !ok {
		return nil, fmt.Errorf(errorUnsupportedOperatorInt64, f.Operator)
	}
	return func() (bool, error) {
		var value int64
		if _, err := f.scanFunc(&value); err != nil {
			return false, err
		}
		return cmp(value, data), nil
	}, nil
}

func unsupportedLikeInt64(op operator) (filterFn, error) {
	return func() (bool, error) {
		return false, fmt.Errorf("%s operator is not applicable for int64", op)
	}, nil
}

func containsInt64(f *Filter, shouldContain bool) (filterFn, error) {
	values, err := extractInt64Slice(f.Value)
	if err != nil || len(values) == 0 {
		return nil, fmt.Errorf("operator %s requires a non-empty slice of int64", f.Operator)
	}
	return func() (bool, error) {
		var value int64
		if _, err := f.scanFunc(&value); err != nil {
			return false, err
		}
		found := slices.Contains(values, value)
		if shouldContain {
			return found, nil
		}
		return !found, nil
	}, nil
}

func isNullInt64(f *Filter, expectNull bool) (filterFn, error) {
	return func() (bool, error) {
		var value int64
		ok, _ := f.scanFunc(&value)
		return expectNull != ok, nil
	}, nil
}

func betweenInt64(f *Filter, inclusive bool) (filterFn, error) {
	minVal, maxVal, err := extractRangeInt64(f.Value)
	if err != nil || minVal > maxVal {
		return nil, fmt.Errorf("invalid range for %s operator", f.Operator)
	}
	return func() (bool, error) {
		var value int64
		if _, err := f.scanFunc(&value); err != nil {
			return false, err
		}
		if inclusive {
			return value >= minVal && value <= maxVal, nil
		}
		return value < minVal || value > maxVal, nil
	}, nil
}

func extractInt64Slice(value interface{}) ([]int64, error) {
	raw, ok := value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("value must be slice of interface{}")
	}
	res := make([]int64, len(raw))
	for i, v := range raw {
		vInt, ok := v.(int64)
		if !ok {
			return nil, fmt.Errorf("value %v is not int64", v)
		}
		res[i] = vInt
	}
	return res, nil
}

func extractRangeInt64(value interface{}) (int64, int64, error) {
	raw, ok := value.([]interface{})
	if !ok || len(raw) != 2 {
		return 0, 0, fmt.Errorf("value must be [min, max]")
	}
	min, ok1 := raw[0].(int64)
	max, ok2 := raw[1].(int64)
	if !ok1 || !ok2 {
		return 0, 0, fmt.Errorf("range values must be int64")
	}
	return min, max, nil
}
