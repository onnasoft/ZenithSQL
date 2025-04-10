package filters

import (
	"fmt"
	"slices"
)

const errorUnsupportedOperatorInt64 = "unsupported operator %s for type int64"

func filterInt64(f *Filter) (filterFn, error) {
	switch f.Operator {
	case Equal:
		data, ok := f.Value.(int64)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorInt64, f.Operator)
		}
		return func() (bool, error) {
			var value int64
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value == data, nil
		}, nil
	case NotEqual:
		data, ok := f.Value.(int64)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorInt64, f.Operator)
		}
		return func() (bool, error) {
			var value int64
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value != data, nil
		}, nil
	case GreaterThan:
		data, ok := f.Value.(int64)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorInt64, f.Operator)
		}
		return func() (bool, error) {
			var value int64
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value > data, nil
		}, nil
	case GreaterThanOrEqual:
		data, ok := f.Value.(int64)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorInt64, f.Operator)
		}
		return func() (bool, error) {
			var value int64
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value >= data, nil
		}, nil
	case LessThan:
		data, ok := f.Value.(int64)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorInt64, f.Operator)
		}
		return func() (bool, error) {
			var value int64
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value < data, nil
		}, nil
	case LessThanOrEqual:
		data, ok := f.Value.(int64)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorInt64, f.Operator)
		}
		return func() (bool, error) {
			var value int64
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value <= data, nil
		}, nil
	case Like, NotLike:
		return func() (bool, error) {
			return false, fmt.Errorf("%s operator is not applicable for int64", f.Operator)
		}, nil
	case In:
		values, err := extractInt64Slice(f.Value)
		if err != nil {
			return nil, fmt.Errorf("operator %s requires a slice of values: %v", f.Operator, err)
		}
		if len(values) == 0 {
			return nil, fmt.Errorf("operator %s requires a non-empty slice of values", f.Operator)
		}
		return func() (bool, error) {
			var value int64
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return slices.Contains(values, value), nil
		}, nil
	case NotIn:
		values, err := extractInt64Slice(f.Value)
		if err != nil {
			return nil, fmt.Errorf("operator %s requires a slice of values: %v", f.Operator, err)
		}
		if len(values) == 0 {
			return nil, fmt.Errorf("operator %s requires a non-empty slice of values", f.Operator)
		}
		return func() (bool, error) {
			var value int64
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return !slices.Contains(values, value), nil
		}, nil
	case IsNull:
		return func() (bool, error) {
			var value int64
			ok, _ := f.scanFunc(&value)
			return !ok, nil
		}, nil
	case IsNotNull:
		return func() (bool, error) {
			var value int64
			ok, _ := f.scanFunc(&value)
			return ok, nil
		}, nil
	case Between:
		minVal, maxVal, err := extractRangeInt64(f.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid range for BETWEEN operator: %v", err)
		}
		if minVal > maxVal {
			return nil, fmt.Errorf("invalid range for BETWEEN operator: min > max")
		}
		return func() (bool, error) {
			var value int64
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value >= minVal && value <= maxVal, nil
		}, nil
	case NotBetween:
		minVal, maxVal, err := extractRangeInt64(f.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid range for NOT BETWEEN operator: %v", err)
		}
		if minVal > maxVal {
			return nil, fmt.Errorf("invalid range for NOT BETWEEN operator: min > max")
		}
		return func() (bool, error) {
			var value int64
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value < minVal || value > maxVal, nil
		}, nil
	default:
		return nil, fmt.Errorf(errorUnsupportedOperatorInt64, f.Operator)
	}
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
