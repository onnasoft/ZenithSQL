package filters

import (
	"fmt"
	"slices"
)

const errorUnsupportedOperatorUint64 = "unsupported operator %s for type uint64"

func filterUint64(f *Filter) (filterFn, error) {
	switch f.Operator {
	case Equal:
		return compareUint64(f, func(a, b uint64) bool { return a == b })
	case NotEqual:
		return compareUint64(f, func(a, b uint64) bool { return a != b })
	case GreaterThan:
		return compareUint64(f, func(a, b uint64) bool { return a > b })
	case GreaterThanOrEqual:
		return compareUint64(f, func(a, b uint64) bool { return a >= b })
	case LessThan:
		return compareUint64(f, func(a, b uint64) bool { return a < b })
	case LessThanOrEqual:
		return compareUint64(f, func(a, b uint64) bool { return a <= b })
	case Like, NotLike:
		return unsupportedLikeUint64(f.Operator)
	case In:
		return containsUint64(f, true)
	case NotIn:
		return containsUint64(f, false)
	case IsNull:
		return isNullUint64(f, true)
	case IsNotNull:
		return isNullUint64(f, false)
	case Between:
		return betweenUint64(f, true)
	case NotBetween:
		return betweenUint64(f, false)
	default:
		return nil, fmt.Errorf(errorUnsupportedOperatorUint64, f.Operator)
	}
}

func compareUint64(f *Filter, cmp func(a, b uint64) bool) (filterFn, error) {
	data, ok := f.Value.(uint64)
	if !ok {
		return nil, fmt.Errorf(errorUnsupportedOperatorUint64, f.Operator)
	}
	return func() (bool, error) {
		var value uint64
		if _, err := f.scanFunc(&value); err != nil {
			return false, err
		}
		return cmp(value, data), nil
	}, nil
}

func unsupportedLikeUint64(op operator) (filterFn, error) {
	return func() (bool, error) {
		return false, fmt.Errorf("%s operator is not applicable for uint64", op)
	}, nil
}

func containsUint64(f *Filter, shouldContain bool) (filterFn, error) {
	values, err := extractUint64Slice(f.Value)
	if err != nil || len(values) == 0 {
		return nil, fmt.Errorf("operator %s requires a non-empty slice of uint64", f.Operator)
	}
	return func() (bool, error) {
		var value uint64
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

func isNullUint64(f *Filter, expectNull bool) (filterFn, error) {
	return func() (bool, error) {
		var value uint64
		ok, _ := f.scanFunc(&value)
		return expectNull != ok, nil
	}, nil
}

func betweenUint64(f *Filter, inclusive bool) (filterFn, error) {
	minVal, maxVal, err := extractRangeUint64(f.Value)
	if err != nil || minVal > maxVal {
		return nil, fmt.Errorf("invalid range for %s operator", f.Operator)
	}
	return func() (bool, error) {
		var value uint64
		if _, err := f.scanFunc(&value); err != nil {
			return false, err
		}
		if inclusive {
			return value >= minVal && value <= maxVal, nil
		}
		return value < minVal || value > maxVal, nil
	}, nil
}

func extractUint64Slice(value interface{}) ([]uint64, error) {
	raw, ok := value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("value must be slice of interface{}")
	}
	res := make([]uint64, len(raw))
	for i, v := range raw {
		vInt, ok := v.(uint64)
		if !ok {
			return nil, fmt.Errorf("value %v is not uint64", v)
		}
		res[i] = vInt
	}
	return res, nil
}

func extractRangeUint64(value interface{}) (uint64, uint64, error) {
	raw, ok := value.([]interface{})
	if !ok || len(raw) != 2 {
		return 0, 0, fmt.Errorf("value must be [min, max]")
	}
	min, ok1 := raw[0].(uint64)
	max, ok2 := raw[1].(uint64)
	if !ok1 || !ok2 {
		return 0, 0, fmt.Errorf("range values must be uint64")
	}
	return min, max, nil
}
