package filters

import (
	"fmt"
	"slices"
)

const errorUnsupportedOperatorInt8 = "unsupported operator %s for type int8"

func filterInt8(f *Filter) (filterFn, error) {
	switch f.Operator {
	case Equal:
		data, ok := f.Value.(int8)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorInt8, f.Operator)
		}
		return func() (bool, error) {
			var value int8
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value == data, nil
		}, nil
	case NotEqual:
		data, ok := f.Value.(int8)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorInt8, f.Operator)
		}
		return func() (bool, error) {
			var value int8
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value != data, nil
		}, nil
	case GreaterThan:
		data, ok := f.Value.(int8)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorInt8, f.Operator)
		}
		return func() (bool, error) {
			var value int8
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value > data, nil
		}, nil
	case GreaterThanOrEqual:
		data, ok := f.Value.(int8)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorInt8, f.Operator)
		}
		return func() (bool, error) {
			var value int8
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value >= data, nil
		}, nil
	case LessThan:
		data, ok := f.Value.(int8)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorInt8, f.Operator)
		}
		return func() (bool, error) {
			var value int8
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value < data, nil
		}, nil
	case LessThanOrEqual:
		data, ok := f.Value.(int8)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorInt8, f.Operator)
		}
		return func() (bool, error) {
			var value int8
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value <= data, nil
		}, nil
	case Like:
		return func() (bool, error) {
			var value int8
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			// Assuming the LIKE operator is not applicable for int8
			// This is a placeholder; you might want to handle this differently
			return false, fmt.Errorf("LIKE operator is not applicable for int8")
		}, nil
	case NotLike:
		return func() (bool, error) {
			var value int8
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			// Assuming the NOT LIKE operator is not applicable for int8
			// This is a placeholder; you might want to handle this differently
			return false, fmt.Errorf("NOT LIKE operator is not applicable for int8")
		}, nil

	case In:
		values, err := extractInt8Slice(f.Value)
		if err != nil {
			return nil, fmt.Errorf("operator %s requires a slice of values", f.Operator)
		}
		if len(values) == 0 {
			return nil, fmt.Errorf("operator %s requires a non-empty slice of values", f.Operator)
		}
		return func() (bool, error) {
			var value int8
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return slices.Contains(values, value), nil
		}, nil

	case NotIn:
		values, err := extractInt8Slice(f.Value)
		if err != nil {
			return nil, fmt.Errorf("operator %s requires a slice of values", f.Operator)
		}
		if len(values) == 0 {
			return nil, fmt.Errorf("operator %s requires a non-empty slice of values", f.Operator)
		}
		return func() (bool, error) {
			var value int8
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return !slices.Contains(values, value), nil
		}, nil

	case IsNull:
		return func() (bool, error) {
			var value int8
			ok, _ := f.scanFunc(&value)
			return !ok, nil
		}, nil
	case IsNotNull:
		return func() (bool, error) {
			var value int8
			ok, _ := f.scanFunc(&value)
			return ok, nil
		}, nil
	case Between:
		minVal, maxVal, err := extractRangeInt8(f.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid range for BETWEEN operator: %v", err)
		}
		if minVal > maxVal {
			return nil, fmt.Errorf("invalid range for BETWEEN operator: min value is greater than max value")
		}

		return func() (bool, error) {
			var value int8
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value >= minVal && value <= maxVal, nil
		}, nil

	case NotBetween:
		minVal, maxVal, err := extractRangeInt8(f.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid range for BETWEEN operator: %v", err)
		}
		if minVal > maxVal {
			return nil, fmt.Errorf("invalid range for BETWEEN operator: min value is greater than max value")
		}

		return func() (bool, error) {
			var value int8
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value < minVal || value > maxVal, nil
		}, nil
	default:
		return func() (bool, error) {
			return false, fmt.Errorf("unsupported operator %s for type int8", f.Operator)
		}, nil
	}
}

func extractInt8Slice(value interface{}) ([]int8, error) {
	raw, ok := value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("value must be slice of interface{}")
	}
	res := make([]int8, len(raw))
	for i, v := range raw {
		vInt, ok := v.(int8)
		if !ok {
			return nil, fmt.Errorf("value %v is not int8", v)
		}
		res[i] = vInt
	}
	return res, nil
}

func extractRangeInt8(value interface{}) (int8, int8, error) {
	raw, ok := value.([]interface{})
	if !ok || len(raw) != 2 {
		return 0, 0, fmt.Errorf("value must be [min, max]")
	}
	min, ok1 := raw[0].(int8)
	max, ok2 := raw[1].(int8)
	if !ok1 || !ok2 {
		return 0, 0, fmt.Errorf("range values must be int8")
	}
	return min, max, nil
}
