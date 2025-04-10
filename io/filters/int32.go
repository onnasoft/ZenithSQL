package filters

import (
	"fmt"
	"slices"
)

const errorUnsupportedOperatorInt32 = "unsupported operator %s for type int32"

func filterInt32(f *Filter) (filterFn, error) {
	switch f.Operator {
	case Equal:
		data, ok := f.Value.(int32)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorInt32, f.Operator)
		}
		return func() (bool, error) {
			var value int32
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value == data, nil
		}, nil
	case NotEqual:
		data, ok := f.Value.(int32)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorInt32, f.Operator)
		}
		return func() (bool, error) {
			var value int32
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value != data, nil
		}, nil
	case GreaterThan:
		data, ok := f.Value.(int32)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorInt32, f.Operator)
		}
		return func() (bool, error) {
			var value int32
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value > data, nil
		}, nil
	case GreaterThanOrEqual:
		data, ok := f.Value.(int32)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorInt32, f.Operator)
		}
		return func() (bool, error) {
			var value int32
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value >= data, nil
		}, nil
	case LessThan:
		data, ok := f.Value.(int32)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorInt32, f.Operator)
		}
		return func() (bool, error) {
			var value int32
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value < data, nil
		}, nil
	case LessThanOrEqual:
		data, ok := f.Value.(int32)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorInt32, f.Operator)
		}
		return func() (bool, error) {
			var value int32
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value <= data, nil
		}, nil
	case Like, NotLike:
		return func() (bool, error) {
			return false, fmt.Errorf("%s operator is not applicable for int32", f.Operator)
		}, nil
	case In:
		values, err := extractInt32Slice(f.Value)
		if err != nil {
			return nil, fmt.Errorf("operator %s requires a slice of values: %v", f.Operator, err)
		}
		if len(values) == 0 {
			return nil, fmt.Errorf("operator %s requires a non-empty slice of values", f.Operator)
		}
		return func() (bool, error) {
			var value int32
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return slices.Contains(values, value), nil
		}, nil
	case NotIn:
		values, err := extractInt32Slice(f.Value)
		if err != nil {
			return nil, fmt.Errorf("operator %s requires a slice of values: %v", f.Operator, err)
		}
		if len(values) == 0 {
			return nil, fmt.Errorf("operator %s requires a non-empty slice of values", f.Operator)
		}
		return func() (bool, error) {
			var value int32
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return !slices.Contains(values, value), nil
		}, nil
	case IsNull:
		return func() (bool, error) {
			var value int32
			ok, _ := f.scanFunc(&value)
			return !ok, nil
		}, nil
	case IsNotNull:
		return func() (bool, error) {
			var value int32
			ok, _ := f.scanFunc(&value)
			return ok, nil
		}, nil
	case Between:
		minVal, maxVal, err := extractRangeInt32(f.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid range for BETWEEN operator: %v", err)
		}
		if minVal > maxVal {
			return nil, fmt.Errorf("invalid range for BETWEEN operator: min > max")
		}
		return func() (bool, error) {
			var value int32
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value >= minVal && value <= maxVal, nil
		}, nil
	case NotBetween:
		minVal, maxVal, err := extractRangeInt32(f.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid range for NOT BETWEEN operator: %v", err)
		}
		if minVal > maxVal {
			return nil, fmt.Errorf("invalid range for NOT BETWEEN operator: min > max")
		}
		return func() (bool, error) {
			var value int32
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value < minVal || value > maxVal, nil
		}, nil
	default:
		return nil, fmt.Errorf(errorUnsupportedOperatorInt32, f.Operator)
	}
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
