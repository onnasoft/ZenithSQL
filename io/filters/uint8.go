package filters

import (
	"fmt"
	"slices"
)

const errorUnsupportedOperatorUint8 = "unsupported operator %s for type uint8"

func filterUint8(f *Filter) (filterFn, error) {
	switch f.Operator {
	case Equal:
		return compareUint8(f, func(a, b uint8) bool { return a == b })
	case NotEqual:
		return compareUint8(f, func(a, b uint8) bool { return a != b })
	case GreaterThan:
		return compareUint8(f, func(a, b uint8) bool { return a > b })
	case GreaterThanOrEqual:
		return compareUint8(f, func(a, b uint8) bool { return a >= b })
	case LessThan:
		return compareUint8(f, func(a, b uint8) bool { return a < b })
	case LessThanOrEqual:
		return compareUint8(f, func(a, b uint8) bool { return a <= b })
	case Like, NotLike:
		return unsupportedLikeUint8(f.Operator)
	case In:
		return containsUint8(f, true)
	case NotIn:
		return containsUint8(f, false)
	case IsNull:
		return isNullUint8(f, true)
	case IsNotNull:
		return isNullUint8(f, false)
	case Between:
		return betweenUint8(f, true)
	case NotBetween:
		return betweenUint8(f, false)
	default:
		return nil, fmt.Errorf(errorUnsupportedOperatorUint8, f.Operator)
	}
}

func compareUint8(f *Filter, cmp func(a, b uint8) bool) (filterFn, error) {
	data, ok := f.Value.(uint8)
	if !ok {
		return nil, fmt.Errorf(errorUnsupportedOperatorUint8, f.Operator)
	}
	return func() (bool, error) {
		var value uint8
		if _, err := f.scanFunc(&value); err != nil {
			return false, err
		}
		return cmp(value, data), nil
	}, nil
}

func unsupportedLikeUint8(op operator) (filterFn, error) {
	return func() (bool, error) {
		return false, fmt.Errorf("%s operator is not applicable for uint8", op)
	}, nil
}

func containsUint8(f *Filter, shouldContain bool) (filterFn, error) {
	values, err := extractUint8Slice(f.Value)
	if err != nil || len(values) == 0 {
		return nil, fmt.Errorf("operator %s requires a non-empty slice of uint8", f.Operator)
	}
	return func() (bool, error) {
		var value uint8
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

func isNullUint8(f *Filter, expectNull bool) (filterFn, error) {
	return func() (bool, error) {
		var value uint8
		ok, _ := f.scanFunc(&value)
		return expectNull != ok, nil
	}, nil
}

func betweenUint8(f *Filter, inclusive bool) (filterFn, error) {
	minVal, maxVal, err := extractRangeUint8(f.Value)
	if err != nil || minVal > maxVal {
		return nil, fmt.Errorf("invalid range for %s operator", f.Operator)
	}
	return func() (bool, error) {
		var value uint8
		if _, err := f.scanFunc(&value); err != nil {
			return false, err
		}
		if inclusive {
			return value >= minVal && value <= maxVal, nil
		}
		return value < minVal || value > maxVal, nil
	}, nil
}

func extractUint8Slice(value interface{}) ([]uint8, error) {
	raw, ok := value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("value must be slice of interface{}")
	}
	res := make([]uint8, len(raw))
	for i, v := range raw {
		vInt, ok := v.(uint8)
		if !ok {
			return nil, fmt.Errorf("value %v is not uint8", v)
		}
		res[i] = vInt
	}
	return res, nil
}

func extractRangeUint8(value interface{}) (uint8, uint8, error) {
	raw, ok := value.([]interface{})
	if !ok || len(raw) != 2 {
		return 0, 0, fmt.Errorf("value must be [min, max]")
	}
	min, ok1 := raw[0].(uint8)
	max, ok2 := raw[1].(uint8)
	if !ok1 || !ok2 {
		return 0, 0, fmt.Errorf("range values must be uint8")
	}
	return min, max, nil
}
