package filters

import (
	"fmt"
	"slices"
)

const errorUnsupportedOperatorInt16 = "unsupported operator %s for type int16"

func filterInt16(f *Filter) (filterFn, error) {
	switch f.Operator {
	case Equal:
		return compareInt16(f, func(a, b int16) bool { return a == b })
	case NotEqual:
		return compareInt16(f, func(a, b int16) bool { return a != b })
	case GreaterThan:
		return compareInt16(f, func(a, b int16) bool { return a > b })
	case GreaterThanOrEqual:
		return compareInt16(f, func(a, b int16) bool { return a >= b })
	case LessThan:
		return compareInt16(f, func(a, b int16) bool { return a < b })
	case LessThanOrEqual:
		return compareInt16(f, func(a, b int16) bool { return a <= b })
	case Like, NotLike:
		return unsupportedLikeInt16(f.Operator)
	case In:
		return containsInt16(f, true)
	case NotIn:
		return containsInt16(f, false)
	case IsNull:
		return isNullInt16(f, true)
	case IsNotNull:
		return isNullInt16(f, false)
	case Between:
		return betweenInt16(f, true)
	case NotBetween:
		return betweenInt16(f, false)
	default:
		return func() (bool, error) {
			return false, fmt.Errorf(errorUnsupportedOperatorInt16, f.Operator)
		}, nil
	}
}

func compareInt16(f *Filter, cmp func(a, b int16) bool) (filterFn, error) {
	data, ok := f.Value.(int16)
	if !ok {
		return nil, fmt.Errorf(errorUnsupportedOperatorInt16, f.Operator)
	}
	return func() (bool, error) {
		var value int16
		if _, err := f.scanFunc(&value); err != nil {
			return false, err
		}
		return cmp(value, data), nil
	}, nil
}

func unsupportedLikeInt16(op operator) (filterFn, error) {
	return func() (bool, error) {
		return false, fmt.Errorf("%s operator is not applicable for int16", op)
	}, nil
}

func containsInt16(f *Filter, shouldContain bool) (filterFn, error) {
	values, err := extractInt16Slice(f.Value)
	if err != nil || len(values) == 0 {
		return nil, fmt.Errorf("operator %s requires a non-empty slice of int16", f.Operator)
	}
	return func() (bool, error) {
		var value int16
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

func isNullInt16(f *Filter, expectNull bool) (filterFn, error) {
	return func() (bool, error) {
		var value int16
		ok, _ := f.scanFunc(&value)
		return expectNull != ok, nil
	}, nil
}

func betweenInt16(f *Filter, inclusive bool) (filterFn, error) {
	minVal, maxVal, err := extractRangeInt16(f.Value)
	if err != nil || minVal > maxVal {
		return nil, fmt.Errorf("invalid range for %s operator", f.Operator)
	}
	return func() (bool, error) {
		var value int16
		if _, err := f.scanFunc(&value); err != nil {
			return false, err
		}
		if inclusive {
			return value >= minVal && value <= maxVal, nil
		}
		return value < minVal || value > maxVal, nil
	}, nil
}

func extractInt16Slice(value interface{}) ([]int16, error) {
	raw, ok := value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("value must be slice of interface{}")
	}
	res := make([]int16, len(raw))
	for i, v := range raw {
		vInt, ok := v.(int16)
		if !ok {
			return nil, fmt.Errorf("value %v is not int16", v)
		}
		res[i] = vInt
	}
	return res, nil
}

func extractRangeInt16(value interface{}) (int16, int16, error) {
	raw, ok := value.([]interface{})
	if !ok || len(raw) != 2 {
		return 0, 0, fmt.Errorf("value must be [min, max]")
	}
	min, ok1 := raw[0].(int16)
	max, ok2 := raw[1].(int16)
	if !ok1 || !ok2 {
		return 0, 0, fmt.Errorf("range values must be int16")
	}
	return min, max, nil
}
