package filters

import (
	"fmt"
	"slices"
)

const errorUnsupportedOperatorUint16 = "unsupported operator %s for type uint16"

func filterUint16(f *Filter) (filterFn, error) {
	switch f.Operator {
	case Equal:
		return compareUint16(f, func(a, b uint16) bool { return a == b })
	case NotEqual:
		return compareUint16(f, func(a, b uint16) bool { return a != b })
	case GreaterThan:
		return compareUint16(f, func(a, b uint16) bool { return a > b })
	case GreaterThanOrEqual:
		return compareUint16(f, func(a, b uint16) bool { return a >= b })
	case LessThan:
		return compareUint16(f, func(a, b uint16) bool { return a < b })
	case LessThanOrEqual:
		return compareUint16(f, func(a, b uint16) bool { return a <= b })
	case Like, NotLike:
		return unsupportedLikeUint16(f.Operator)
	case In:
		return containsUint16(f, true)
	case NotIn:
		return containsUint16(f, false)
	case IsNull:
		return isNullUint16(f, true)
	case IsNotNull:
		return isNullUint16(f, false)
	case Between:
		return betweenUint16(f, true)
	case NotBetween:
		return betweenUint16(f, false)
	default:
		return nil, fmt.Errorf(errorUnsupportedOperatorUint16, f.Operator)
	}
}

func compareUint16(f *Filter, cmp func(a, b uint16) bool) (filterFn, error) {
	data, ok := f.Value.(uint16)
	if !ok {
		return nil, fmt.Errorf(errorUnsupportedOperatorUint16, f.Operator)
	}
	return func() (bool, error) {
		var value uint16
		if _, err := f.scanFunc(&value); err != nil {
			return false, err
		}
		return cmp(value, data), nil
	}, nil
}

func unsupportedLikeUint16(op operator) (filterFn, error) {
	return func() (bool, error) {
		return false, fmt.Errorf("%s operator is not applicable for uint16", op)
	}, nil
}

func containsUint16(f *Filter, shouldContain bool) (filterFn, error) {
	values, err := extractUint16Slice(f.Value)
	if err != nil || len(values) == 0 {
		return nil, fmt.Errorf("operator %s requires a non-empty slice of uint16", f.Operator)
	}
	return func() (bool, error) {
		var value uint16
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

func isNullUint16(f *Filter, expectNull bool) (filterFn, error) {
	return func() (bool, error) {
		var value uint16
		ok, _ := f.scanFunc(&value)
		return expectNull != ok, nil
	}, nil
}

func betweenUint16(f *Filter, inclusive bool) (filterFn, error) {
	minVal, maxVal, err := extractRangeUint16(f.Value)
	if err != nil || minVal > maxVal {
		return nil, fmt.Errorf("invalid range for %s operator", f.Operator)
	}
	return func() (bool, error) {
		var value uint16
		if _, err := f.scanFunc(&value); err != nil {
			return false, err
		}
		if inclusive {
			return value >= minVal && value <= maxVal, nil
		}
		return value < minVal || value > maxVal, nil
	}, nil
}

func extractUint16Slice(value interface{}) ([]uint16, error) {
	raw, ok := value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("value must be slice of interface{}")
	}
	res := make([]uint16, len(raw))
	for i, v := range raw {
		vInt, ok := v.(uint16)
		if !ok {
			return nil, fmt.Errorf("value %v is not uint16", v)
		}
		res[i] = vInt
	}
	return res, nil
}

func extractRangeUint16(value interface{}) (uint16, uint16, error) {
	raw, ok := value.([]interface{})
	if !ok || len(raw) != 2 {
		return 0, 0, fmt.Errorf("value must be [min, max]")
	}
	min, ok1 := raw[0].(uint16)
	max, ok2 := raw[1].(uint16)
	if !ok1 || !ok2 {
		return 0, 0, fmt.Errorf("range values must be uint16")
	}
	return min, max, nil
}
