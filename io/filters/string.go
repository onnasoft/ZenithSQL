package filters

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

const errorUnsupportedOperatorString = "unsupported operator %s for type string"

func filterString(f *Filter) (filterFn, error) {
	switch f.Operator {
	case Equal:
		return compareString(f, func(a, b string) bool { return a == b })
	case NotEqual:
		return compareString(f, func(a, b string) bool { return a != b })
	case GreaterThan:
		return compareString(f, func(a, b string) bool { return a > b })
	case GreaterThanOrEqual:
		return compareString(f, func(a, b string) bool { return a >= b })
	case LessThan:
		return compareString(f, func(a, b string) bool { return a < b })
	case LessThanOrEqual:
		return compareString(f, func(a, b string) bool { return a <= b })
	case Like:
		return likeMatchString(f, true)
	case NotLike:
		return likeMatchString(f, false)
	case In:
		return containsString(f, true)
	case NotIn:
		return containsString(f, false)
	case IsNull:
		return isNullString(f, true)
	case IsNotNull:
		return isNullString(f, false)
	case Between:
		return betweenString(f, true)
	case NotBetween:
		return betweenString(f, false)
	default:
		return nil, fmt.Errorf(errorUnsupportedOperatorString, f.Operator)
	}
}

func compareString(f *Filter, cmp func(a, b string) bool) (filterFn, error) {
	data, ok := f.Value.(string)
	if !ok {
		return nil, fmt.Errorf(errorUnsupportedOperatorString, f.Operator)
	}
	return func() (bool, error) {
		var value string
		if _, err := f.scanFunc(&value); err != nil {
			return false, err
		}
		return cmp(value, data), nil
	}, nil
}

func likeMatchString(f *Filter, positive bool) (filterFn, error) {
	pattern, ok := f.Value.(string)
	if !ok {
		return nil, fmt.Errorf("%s operator requires a string pattern", f.Operator)
	}
	re, err := regexp.Compile(likeToRegex(pattern))
	if err != nil {
		return nil, fmt.Errorf("invalid LIKE pattern: %v", err)
	}
	return func() (bool, error) {
		var value string
		if _, err := f.scanFunc(&value); err != nil {
			return false, err
		}
		matched := re.MatchString(value)
		if positive {
			return matched, nil
		}
		return !matched, nil
	}, nil
}

func containsString(f *Filter, shouldContain bool) (filterFn, error) {
	values, err := extractStringSlice(f.Value)
	if err != nil || len(values) == 0 {
		return nil, fmt.Errorf("operator %s requires a non-empty slice of strings", f.Operator)
	}
	return func() (bool, error) {
		var value string
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

func isNullString(f *Filter, expectNull bool) (filterFn, error) {
	return func() (bool, error) {
		var value string
		ok, _ := f.scanFunc(&value)
		return expectNull != ok, nil
	}, nil
}

func betweenString(f *Filter, inclusive bool) (filterFn, error) {
	minVal, maxVal, err := extractRangeString(f.Value)
	if err != nil || minVal > maxVal {
		return nil, fmt.Errorf("invalid range for %s operator", f.Operator)
	}
	return func() (bool, error) {
		var value string
		if _, err := f.scanFunc(&value); err != nil {
			return false, err
		}
		if inclusive {
			return value >= minVal && value <= maxVal, nil
		}
		return value < minVal || value > maxVal, nil
	}, nil
}

func likeToRegex(pattern string) string {
	p := regexp.QuoteMeta(pattern)
	p = strings.ReplaceAll(p, "%", ".*")
	p = strings.ReplaceAll(p, "_", ".")
	return "^" + p + "$"
}

func extractStringSlice(value interface{}) ([]string, error) {
	raw, ok := value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("value must be slice of interface{}")
	}
	res := make([]string, len(raw))
	for i, v := range raw {
		str, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("value %v is not string", v)
		}
		res[i] = str
	}
	return res, nil
}

func extractRangeString(value interface{}) (string, string, error) {
	raw, ok := value.([]interface{})
	if !ok || len(raw) != 2 {
		return "", "", fmt.Errorf("value must be [min, max]")
	}
	min, ok1 := raw[0].(string)
	max, ok2 := raw[1].(string)
	if !ok1 || !ok2 {
		return "", "", fmt.Errorf("range values must be string")
	}
	return min, max, nil
}
