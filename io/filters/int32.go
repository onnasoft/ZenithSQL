package filters

import (
	"fmt"
	"slices"
)

const errorUnsupportedOperatorInt32 = "unsupported operator %s for type int32"

func filterInt32(f *Filter) (filterFn, error) {
	switch f.Operator {
	case Equal:
		return compareInt32(f, func(a, b int32) bool { return a == b })
	case NotEqual:
		return compareInt32(f, func(a, b int32) bool { return a != b })
	case GreaterThan:
		return compareInt32(f, func(a, b int32) bool { return a > b })
	case GreaterThanOrEqual:
		return compareInt32(f, func(a, b int32) bool { return a >= b })
	case LessThan:
		return compareInt32(f, func(a, b int32) bool { return a < b })
	case LessThanOrEqual:
		return compareInt32(f, func(a, b int32) bool { return a <= b })
	case Like, NotLike:
		return unsupportedLikeInt32(f.Operator)
	case In:
		return containsInt32(f, true)
	case NotIn:
		return containsInt32(f, false)
	case IsNull:
		return isNullInt32(f, true)
	case IsNotNull:
		return isNullInt32(f, false)
	case Between:
		return betweenInt32(f, true)
	case NotBetween:
		return betweenInt32(f, false)
	default:
		return nil, fmt.Errorf(errorUnsupportedOperatorInt32, f.Operator)
	}
}

func compareInt32(f *Filter, cmp func(a, b int32) bool) (filterFn, error) {
	data, ok := f.Value.(int32)
	if !ok {
		return nil, fmt.Errorf(errorUnsupportedOperatorInt32, f.Operator)
	}
	return func() (bool, error) {
		var value int32
		if _, err := f.scanFunc(&value); err != nil {
			return false, err
		}
		return cmp(value, data), nil
	}, nil
}

func unsupportedLikeInt32(op operator) (filterFn, error) {
	return func() (bool, error) {
		return false, fmt.Errorf("%s operator is not applicable for int32", op)
	}, nil
}

func containsInt32(f *Filter, shouldContain bool) (filterFn, error) {
	values, err := extractInt32Slice(f.Value)
	if err != nil || len(values) == 0 {
		return nil, fmt.Errorf("operator %s requires a non-empty slice of int32", f.Operator)
	}
	return func() (bool, error) {
		var value int32
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

func isNullInt32(f *Filter, expectNull bool) (filterFn, error) {
	return func() (bool, error) {
		var value int32
		ok, _ := f.scanFunc(&value)
		return expectNull != ok, nil
	}, nil
}

func betweenInt32(f *Filter, inclusive bool) (filterFn, error) {
	minVal, maxVal, err := extractRangeInt32(f.Value)
	if err != nil || minVal > maxVal {
		return nil, fmt.Errorf("invalid range for %s operator", f.Operator)
	}
	return func() (bool, error) {
		var value int32
		if _, err := f.scanFunc(&value); err != nil {
			return false, err
		}
		if inclusive {
			return value >= minVal && value <= maxVal, nil
		}
		return value < minVal || value > maxVal, nil
	}, nil
}

func extractInt32Slice(value interface{}) ([]int32, error) {
	raw, ok := value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("value must be slice of interface{}")
	}
	res := make([]int32, len(raw))
	for i, v := range raw {
		vInt, ok := v.(int32)
		if !ok {
			return nil, fmt.Errorf("value %v is not int32", v)
		}
		res[i] = vInt
	}
	return res, nil
}

func extractRangeInt32(value interface{}) (int32, int32, error) {
	raw, ok := value.([]interface{})
	if !ok || len(raw) != 2 {
		return 0, 0, fmt.Errorf("value must be [min, max]")
	}
	min, ok1 := raw[0].(int32)
	max, ok2 := raw[1].(int32)
	if !ok1 || !ok2 {
		return 0, 0, fmt.Errorf("range values must be int32")
	}
	return min, max, nil
}
