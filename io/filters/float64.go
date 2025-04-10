package filters

import (
	"fmt"
	"slices"
)

const errorUnsupportedOperatorFloat64 = "unsupported operator %s for type float64"

func filterFloat64(f *Filter) (filterFn, error) {
	switch f.Operator {
	case Equal:
		return compareFloat64(f, func(a, b float64) bool { return a == b })
	case NotEqual:
		return compareFloat64(f, func(a, b float64) bool { return a != b })
	case GreaterThan:
		return compareFloat64(f, func(a, b float64) bool { return a > b })
	case GreaterThanOrEqual:
		return compareFloat64(f, func(a, b float64) bool { return a >= b })
	case LessThan:
		return compareFloat64(f, func(a, b float64) bool { return a < b })
	case LessThanOrEqual:
		return compareFloat64(f, func(a, b float64) bool { return a <= b })
	case Like, NotLike:
		return unsupportedLikeFloat64(f.Operator)
	case In:
		return containsFloat64(f, true)
	case NotIn:
		return containsFloat64(f, false)
	case IsNull:
		return isNullFloat64(f, true)
	case IsNotNull:
		return isNullFloat64(f, false)
	case Between:
		return betweenFloat64(f, true)
	case NotBetween:
		return betweenFloat64(f, false)
	default:
		return func() (bool, error) {
			return false, fmt.Errorf(errorUnsupportedOperatorFloat64, f.Operator)
		}, nil
	}
}

func compareFloat64(f *Filter, cmp func(a, b float64) bool) (filterFn, error) {
	data, ok := f.Value.(float64)
	if !ok {
		return nil, fmt.Errorf(errorUnsupportedOperatorFloat64, f.Operator)
	}
	return func() (bool, error) {
		var value float64
		if _, err := f.scanFunc(&value); err != nil {
			return false, err
		}
		return cmp(value, data), nil
	}, nil
}

func unsupportedLikeFloat64(op operator) (filterFn, error) {
	return func() (bool, error) {
		return false, fmt.Errorf("%s operator is not applicable for float64", op)
	}, nil
}

func containsFloat64(f *Filter, shouldContain bool) (filterFn, error) {
	values, err := extractFloat64Slice(f.Value)
	if err != nil || len(values) == 0 {
		return nil, fmt.Errorf("operator %s requires a non-empty slice of float64", f.Operator)
	}
	return func() (bool, error) {
		var value float64
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

func isNullFloat64(f *Filter, expectNull bool) (filterFn, error) {
	return func() (bool, error) {
		var value float64
		ok, _ := f.scanFunc(&value)
		return expectNull != ok, nil
	}, nil
}

func betweenFloat64(f *Filter, inclusive bool) (filterFn, error) {
	minVal, maxVal, err := extractRangeFloat64(f.Value)
	if err != nil || minVal > maxVal {
		return nil, fmt.Errorf("invalid range for %s operator", f.Operator)
	}
	return func() (bool, error) {
		var value float64
		if _, err := f.scanFunc(&value); err != nil {
			return false, err
		}
		if inclusive {
			return value >= minVal && value <= maxVal, nil
		}
		return value < minVal || value > maxVal, nil
	}, nil
}

func extractFloat64Slice(value interface{}) ([]float64, error) {
	raw, ok := value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("value must be slice of interface{}")
	}
	res := make([]float64, len(raw))
	for i, v := range raw {
		vFloat, ok := v.(float64)
		if !ok {
			return nil, fmt.Errorf("value %v is not float64", v)
		}
		res[i] = vFloat
	}
	return res, nil
}

func extractRangeFloat64(value interface{}) (float64, float64, error) {
	raw, ok := value.([]interface{})
	if !ok || len(raw) != 2 {
		return 0, 0, fmt.Errorf("value must be [min, max]")
	}
	min, ok1 := raw[0].(float64)
	max, ok2 := raw[1].(float64)
	if !ok1 || !ok2 {
		return 0, 0, fmt.Errorf("range values must be float64")
	}
	return min, max, nil
}
