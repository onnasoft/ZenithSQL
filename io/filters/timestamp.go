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
		return compareTimestamp(f, func(a, b time.Time) bool { return a.Equal(b) })
	case NotEqual:
		return compareTimestamp(f, func(a, b time.Time) bool { return !a.Equal(b) })
	case GreaterThan:
		return compareTimestamp(f, func(a, b time.Time) bool { return a.After(b) })
	case GreaterThanOrEqual:
		return compareTimestamp(f, func(a, b time.Time) bool { return a.After(b) || a.Equal(b) })
	case LessThan:
		return compareTimestamp(f, func(a, b time.Time) bool { return a.Before(b) })
	case LessThanOrEqual:
		return compareTimestamp(f, func(a, b time.Time) bool { return a.Before(b) || a.Equal(b) })
	case Like, NotLike:
		return unsupportedLikeTimestamp(f.Operator)
	case In:
		return containsTimestamp(f, true)
	case NotIn:
		return containsTimestamp(f, false)
	case IsNull:
		return isNullTimestamp(f, true)
	case IsNotNull:
		return isNullTimestamp(f, false)
	case Between:
		return betweenTimestamp(f, true)
	case NotBetween:
		return betweenTimestamp(f, false)
	default:
		return nil, fmt.Errorf(errorUnsupportedOperatorTimestamp, f.Operator)
	}
}

func compareTimestamp(f *Filter, cmp func(a, b time.Time) bool) (filterFn, error) {
	data, ok := f.Value.(time.Time)
	if !ok {
		return nil, fmt.Errorf(errorUnsupportedOperatorTimestamp, f.Operator)
	}
	return func() (bool, error) {
		var value time.Time
		if _, err := f.scanFunc(&value); err != nil {
			return false, err
		}
		return cmp(value, data), nil
	}, nil
}

func containsTimestamp(f *Filter, shouldContain bool) (filterFn, error) {
	values, err := extractTimestampSlice(f.Value)
	if err != nil || len(values) == 0 {
		return nil, fmt.Errorf("operator %s requires a non-empty slice of time.Time", f.Operator)
	}
	return func() (bool, error) {
		var value time.Time
		if _, err := f.scanFunc(&value); err != nil {
			return false, err
		}
		found := slices.ContainsFunc(values, func(t time.Time) bool {
			return t.Equal(value)
		})
		if shouldContain {
			return found, nil
		}
		return !found, nil
	}, nil
}

func isNullTimestamp(f *Filter, expectNull bool) (filterFn, error) {
	return func() (bool, error) {
		var value time.Time
		ok, _ := f.scanFunc(&value)
		return expectNull != ok, nil
	}, nil
}

func betweenTimestamp(f *Filter, inclusive bool) (filterFn, error) {
	minVal, maxVal, err := extractRangeTimestamp(f.Value)
	if err != nil || minVal.After(maxVal) {
		return nil, fmt.Errorf("invalid range for %s operator", f.Operator)
	}
	return func() (bool, error) {
		var value time.Time
		if _, err := f.scanFunc(&value); err != nil {
			return false, err
		}
		if inclusive {
			return (value.After(minVal) || value.Equal(minVal)) && (value.Before(maxVal) || value.Equal(maxVal)), nil
		}
		return value.Before(minVal) || value.After(maxVal), nil
	}, nil
}

func unsupportedLikeTimestamp(op operator) (filterFn, error) {
	return func() (bool, error) {
		return false, fmt.Errorf("%s operator is not applicable for timestamp", op)
	}, nil
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
