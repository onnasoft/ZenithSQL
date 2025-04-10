package filters

import (
	"fmt"
	"slices"
	"time"
)

const errorUnsupportedOperatorTimestamp = "unsupported operator %s for type timestamp"

func filterTimestamp(f *Filter) (filterFn, error) {
	switch f.Operator {
	case Equal:
		data, ok := f.Value.(time.Time)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorTimestamp, f.Operator)
		}
		return func() (bool, error) {
			var value time.Time
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value.Equal(data), nil
		}, nil
	case NotEqual:
		data, ok := f.Value.(time.Time)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorTimestamp, f.Operator)
		}
		return func() (bool, error) {
			var value time.Time
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return !value.Equal(data), nil
		}, nil
	case GreaterThan:
		data, ok := f.Value.(time.Time)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorTimestamp, f.Operator)
		}
		return func() (bool, error) {
			var value time.Time
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value.After(data), nil
		}, nil
	case GreaterThanOrEqual:
		data, ok := f.Value.(time.Time)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorTimestamp, f.Operator)
		}
		return func() (bool, error) {
			var value time.Time
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value.After(data) || value.Equal(data), nil
		}, nil
	case LessThan:
		data, ok := f.Value.(time.Time)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorTimestamp, f.Operator)
		}
		return func() (bool, error) {
			var value time.Time
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value.Before(data), nil
		}, nil
	case LessThanOrEqual:
		data, ok := f.Value.(time.Time)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorTimestamp, f.Operator)
		}
		return func() (bool, error) {
			var value time.Time
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value.Before(data) || value.Equal(data), nil
		}, nil
	case Like, NotLike:
		return func() (bool, error) {
			return false, fmt.Errorf("%s operator is not applicable for timestamp", f.Operator)
		}, nil
	case In:
		values, err := extractTimestampSlice(f.Value)
		if err != nil {
			return nil, fmt.Errorf("operator %s requires a slice of timestamps: %v", f.Operator, err)
		}
		if len(values) == 0 {
			return nil, fmt.Errorf("operator %s requires a non-empty slice of values", f.Operator)
		}
		return func() (bool, error) {
			var value time.Time
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return slices.ContainsFunc(values, func(t time.Time) bool {
				return t.Equal(value)
			}), nil
		}, nil
	case NotIn:
		values, err := extractTimestampSlice(f.Value)
		if err != nil {
			return nil, fmt.Errorf("operator %s requires a slice of timestamps: %v", f.Operator, err)
		}
		if len(values) == 0 {
			return nil, fmt.Errorf("operator %s requires a non-empty slice of values", f.Operator)
		}
		return func() (bool, error) {
			var value time.Time
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return !slices.ContainsFunc(values, func(t time.Time) bool {
				return t.Equal(value)
			}), nil
		}, nil
	case IsNull:
		return func() (bool, error) {
			var value time.Time
			ok, _ := f.scanFunc(&value)
			return !ok, nil
		}, nil
	case IsNotNull:
		return func() (bool, error) {
			var value time.Time
			ok, _ := f.scanFunc(&value)
			return ok, nil
		}, nil
	case Between:
		minVal, maxVal, err := extractRangeTimestamp(f.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid range for BETWEEN operator: %v", err)
		}
		if minVal.After(maxVal) {
			return nil, fmt.Errorf("invalid range for BETWEEN operator: min > max")
		}
		return func() (bool, error) {
			var value time.Time
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return (value.After(minVal) || value.Equal(minVal)) &&
				(value.Before(maxVal) || value.Equal(maxVal)), nil
		}, nil
	case NotBetween:
		minVal, maxVal, err := extractRangeTimestamp(f.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid range for NOT BETWEEN operator: %v", err)
		}
		if minVal.After(maxVal) {
			return nil, fmt.Errorf("invalid range for NOT BETWEEN operator: min > max")
		}
		return func() (bool, error) {
			var value time.Time
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value.Before(minVal) || value.After(maxVal), nil
		}, nil
	default:
		return nil, fmt.Errorf(errorUnsupportedOperatorTimestamp, f.Operator)
	}
}

func extractTimestampSlice(value interface{}) ([]time.Time, error) {
	raw, ok := value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("value must be slice of interface{}")
	}
	res := make([]time.Time, len(raw))
	for i, v := range raw {
		vTime, ok := v.(time.Time)
		if !ok {
			return nil, fmt.Errorf("value %v is not time.Time", v)
		}
		res[i] = vTime
	}
	return res, nil
}

func extractRangeTimestamp(value interface{}) (time.Time, time.Time, error) {
	raw, ok := value.([]interface{})
	if !ok || len(raw) != 2 {
		return time.Time{}, time.Time{}, fmt.Errorf("value must be [min, max]")
	}
	min, ok1 := raw[0].(time.Time)
	max, ok2 := raw[1].(time.Time)
	if !ok1 || !ok2 {
		return time.Time{}, time.Time{}, fmt.Errorf("range values must be time.Time")
	}
	return min, max, nil
}
