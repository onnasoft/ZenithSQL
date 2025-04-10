package filters

import (
	"fmt"
	"slices"
)

const errorUnsupportedOperatorFloat64 = "unsupported operator %s for type float64"

func filterFloat64(f *Filter) (filterFn, error) {
	switch f.Operator {
	case Equal:
		data, ok := f.Value.(float64)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorFloat64, f.Operator)
		}
		return func() (bool, error) {
			var value float64
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value == data, nil
		}, nil
	case NotEqual:
		data, ok := f.Value.(float64)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorFloat64, f.Operator)
		}
		return func() (bool, error) {
			var value float64
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value != data, nil
		}, nil
	case GreaterThan:
		data, ok := f.Value.(float64)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorFloat64, f.Operator)
		}
		return func() (bool, error) {
			var value float64
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value > data, nil
		}, nil
	case GreaterThanOrEqual:
		data, ok := f.Value.(float64)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorFloat64, f.Operator)
		}
		return func() (bool, error) {
			var value float64
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value >= data, nil
		}, nil
	case LessThan:
		data, ok := f.Value.(float64)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorFloat64, f.Operator)
		}
		return func() (bool, error) {
			var value float64
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value < data, nil
		}, nil
	case LessThanOrEqual:
		data, ok := f.Value.(float64)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorFloat64, f.Operator)
		}
		return func() (bool, error) {
			var value float64
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value <= data, nil
		}, nil
	case Like, NotLike:
		return func() (bool, error) {
			return false, fmt.Errorf("%s operator is not applicable for float64", f.Operator)
		}, nil
	case In:
		values, err := extractFloat64Slice(f.Value)
		if err != nil {
			return nil, fmt.Errorf("operator %s requires a slice of values: %v", f.Operator, err)
		}
		if len(values) == 0 {
			return nil, fmt.Errorf("operator %s requires a non-empty slice of values", f.Operator)
		}
		return func() (bool, error) {
			var value float64
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return slices.Contains(values, value), nil
		}, nil
	case NotIn:
		values, err := extractFloat64Slice(f.Value)
		if err != nil {
			return nil, fmt.Errorf("operator %s requires a slice of values: %v", f.Operator, err)
		}
		if len(values) == 0 {
			return nil, fmt.Errorf("operator %s requires a non-empty slice of values", f.Operator)
		}
		return func() (bool, error) {
			var value float64
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return !slices.Contains(values, value), nil
		}, nil
	case IsNull:
		return func() (bool, error) {
			var value float64
			ok, _ := f.scanFunc(&value)
			return !ok, nil
		}, nil
	case IsNotNull:
		return func() (bool, error) {
			var value float64
			ok, _ := f.scanFunc(&value)
			return ok, nil
		}, nil
	case Between:
		minVal, maxVal, err := extractRangeFloat64(f.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid range for BETWEEN operator: %v", err)
		}
		if minVal > maxVal {
			return nil, fmt.Errorf("invalid range for BETWEEN operator: min > max")
		}
		return func() (bool, error) {
			var value float64
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value >= minVal && value <= maxVal, nil
		}, nil
	case NotBetween:
		minVal, maxVal, err := extractRangeFloat64(f.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid range for NOT BETWEEN operator: %v", err)
		}
		if minVal > maxVal {
			return nil, fmt.Errorf("invalid range for NOT BETWEEN operator: min > max")
		}
		return func() (bool, error) {
			var value float64
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value < minVal || value > maxVal, nil
		}, nil
	default:
		return nil, fmt.Errorf(errorUnsupportedOperatorFloat64, f.Operator)
	}
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
