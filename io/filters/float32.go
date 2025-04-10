package filters

import (
	"fmt"
	"slices"
)

const errorUnsupportedOperatorFloat32 = "unsupported operator %s for type float32"

func filterFloat32(f *Filter) (filterFn, error) {
	switch f.Operator {
	case Equal:
		return compareFloat32(f, func(a, b float32) bool { return a == b })
	case NotEqual:
		return compareFloat32(f, func(a, b float32) bool { return a != b })
	case GreaterThan:
		return compareFloat32(f, func(a, b float32) bool { return a > b })
	case GreaterThanOrEqual:
		return compareFloat32(f, func(a, b float32) bool { return a >= b })
	case LessThan:
		return compareFloat32(f, func(a, b float32) bool { return a < b })
	case LessThanOrEqual:
		return compareFloat32(f, func(a, b float32) bool { return a <= b })
	case Like, NotLike:
		return unsupportedLikeFloat32(f.Operator)
	case In:
		return containsFloat32(f, true)
	case NotIn:
		return containsFloat32(f, false)
	case IsNull:
		return isNullFloat32(f, true)
	case IsNotNull:
		return isNullFloat32(f, false)
	case Between:
		return betweenFloat32(f, true)
	case NotBetween:
		return betweenFloat32(f, false)
	default:
		return func() (bool, error) {
			return false, fmt.Errorf(errorUnsupportedOperatorFloat32, f.Operator)
		}, nil
	}
}

func compareFloat32(f *Filter, cmp func(a, b float32) bool) (filterFn, error) {
	data, ok := f.Value.(float32)
	if !ok {
		return nil, fmt.Errorf(errorUnsupportedOperatorFloat32, f.Operator)
	}
	return func() (bool, error) {
		var value float32
		if _, err := f.scanFunc(&value); err != nil {
			return false, err
		}
		return cmp(value, data), nil
	}, nil
}

func unsupportedLikeFloat32(op operator) (filterFn, error) {
	return func() (bool, error) {
		return false, fmt.Errorf("%s operator is not applicable for float32", op)
	}, nil
}

func containsFloat32(f *Filter, shouldContain bool) (filterFn, error) {
	values, err := extractFloat32Slice(f.Value)
	if err != nil || len(values) == 0 {
		return nil, fmt.Errorf("operator %s requires a non-empty slice of float32", f.Operator)
	}
	return func() (bool, error) {
		var value float32
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

func isNullFloat32(f *Filter, expectNull bool) (filterFn, error) {
	return func() (bool, error) {
		var value float32
		ok, _ := f.scanFunc(&value)
		return expectNull != ok, nil
	}, nil
}

func betweenFloat32(f *Filter, inclusive bool) (filterFn, error) {
	minVal, maxVal, err := extractRangeFloat32(f.Value)
	if err != nil || minVal > maxVal {
		return nil, fmt.Errorf("invalid range for %s operator", f.Operator)
	}
	return func() (bool, error) {
		var value float32
		if _, err := f.scanFunc(&value); err != nil {
			return false, err
		}
		if inclusive {
			return value >= minVal && value <= maxVal, nil
		}
		return value < minVal || value > maxVal, nil
	}, nil
}

func extractFloat32Slice(value interface{}) ([]float32, error) {
	raw, ok := value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("value must be slice of interface{}")
	}
	res := make([]float32, len(raw))
	for i, v := range raw {
		vFloat, ok := v.(float32)
		if !ok {
			return nil, fmt.Errorf("value %v is not float32", v)
		}
		res[i] = vFloat
	}
	return res, nil
}

func extractRangeFloat32(value interface{}) (float32, float32, error) {
	raw, ok := value.([]interface{})
	if !ok || len(raw) != 2 {
		return 0, 0, fmt.Errorf("value must be [min, max]")
	}
	min, ok1 := raw[0].(float32)
	max, ok2 := raw[1].(float32)
	if !ok1 || !ok2 {
		return 0, 0, fmt.Errorf("range values must be float32")
	}
	return min, max, nil
}
