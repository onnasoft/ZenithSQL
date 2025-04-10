package filters

import (
	"fmt"
	"slices"
)

const errorUnsupportedOperatorUint32 = "unsupported operator %s for type uint32"

func filterUint32(f *Filter) (filterFn, error) {
	switch f.Operator {
	case Equal:
		return compareUint32(f, func(a, b uint32) bool { return a == b })
	case NotEqual:
		return compareUint32(f, func(a, b uint32) bool { return a != b })
	case GreaterThan:
		return compareUint32(f, func(a, b uint32) bool { return a > b })
	case GreaterThanOrEqual:
		return compareUint32(f, func(a, b uint32) bool { return a >= b })
	case LessThan:
		return compareUint32(f, func(a, b uint32) bool { return a < b })
	case LessThanOrEqual:
		return compareUint32(f, func(a, b uint32) bool { return a <= b })
	case Like, NotLike:
		return unsupportedLikeUint32(f.Operator)
	case In:
		return containsUint32(f, true)
	case NotIn:
		return containsUint32(f, false)
	case IsNull:
		return isNullUint32(f, true)
	case IsNotNull:
		return isNullUint32(f, false)
	case Between:
		return betweenUint32(f, true)
	case NotBetween:
		return betweenUint32(f, false)
	default:
		return nil, fmt.Errorf(errorUnsupportedOperatorUint32, f.Operator)
	}
}

func compareUint32(f *Filter, cmp func(a, b uint32) bool) (filterFn, error) {
	data, ok := f.Value.(uint32)
	if !ok {
		return nil, fmt.Errorf(errorUnsupportedOperatorUint32, f.Operator)
	}
	return func() (bool, error) {
		var value uint32
		if _, err := f.scanFunc(&value); err != nil {
			return false, err
		}
		return cmp(value, data), nil
	}, nil
}

func unsupportedLikeUint32(op operator) (filterFn, error) {
	return func() (bool, error) {
		return false, fmt.Errorf("%s operator is not applicable for uint32", op)
	}, nil
}

func containsUint32(f *Filter, shouldContain bool) (filterFn, error) {
	values, err := extractUint32Slice(f.Value)
	if err != nil || len(values) == 0 {
		return nil, fmt.Errorf("operator %s requires a non-empty slice of uint32", f.Operator)
	}
	return func() (bool, error) {
		var value uint32
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

func isNullUint32(f *Filter, expectNull bool) (filterFn, error) {
	return func() (bool, error) {
		var value uint32
		ok, _ := f.scanFunc(&value)
		return expectNull != ok, nil
	}, nil
}

func betweenUint32(f *Filter, inclusive bool) (filterFn, error) {
	minVal, maxVal, err := extractRangeUint32(f.Value)
	if err != nil || minVal > maxVal {
		return nil, fmt.Errorf("invalid range for %s operator", f.Operator)
	}
	return func() (bool, error) {
		var value uint32
		if _, err := f.scanFunc(&value); err != nil {
			return false, err
		}
		if inclusive {
			return value >= minVal && value <= maxVal, nil
		}
		return value < minVal || value > maxVal, nil
	}, nil
}

func extractUint32Slice(value interface{}) ([]uint32, error) {
	raw, ok := value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("value must be slice of interface{}")
	}
	res := make([]uint32, len(raw))
	for i, v := range raw {
		vInt, ok := v.(uint32)
		if !ok {
			return nil, fmt.Errorf("value %v is not uint32", v)
		}
		res[i] = vInt
	}
	return res, nil
}

func extractRangeUint32(value interface{}) (uint32, uint32, error) {
	raw, ok := value.([]interface{})
	if !ok || len(raw) != 2 {
		return 0, 0, fmt.Errorf("value must be [min, max]")
	}
	min, ok1 := raw[0].(uint32)
	max, ok2 := raw[1].(uint32)
	if !ok1 || !ok2 {
		return 0, 0, fmt.Errorf("range values must be uint32")
	}
	return min, max, nil
}
