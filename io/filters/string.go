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
		data, ok := f.Value.(string)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorString, f.Operator)
		}
		return func() (bool, error) {
			var value string
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value == data, nil
		}, nil
	case NotEqual:
		data, ok := f.Value.(string)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorString, f.Operator)
		}
		return func() (bool, error) {
			var value string
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value != data, nil
		}, nil
	case GreaterThan:
		data, ok := f.Value.(string)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorString, f.Operator)
		}
		return func() (bool, error) {
			var value string
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value > data, nil
		}, nil
	case GreaterThanOrEqual:
		data, ok := f.Value.(string)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorString, f.Operator)
		}
		return func() (bool, error) {
			var value string
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value >= data, nil
		}, nil
	case LessThan:
		data, ok := f.Value.(string)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorString, f.Operator)
		}
		return func() (bool, error) {
			var value string
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value < data, nil
		}, nil
	case LessThanOrEqual:
		data, ok := f.Value.(string)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorString, f.Operator)
		}
		return func() (bool, error) {
			var value string
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value <= data, nil
		}, nil
	case Like:
		pattern, ok := f.Value.(string)
		if !ok {
			return nil, fmt.Errorf("LIKE operator requires a string pattern")
		}
		regexPattern := likeToRegex(pattern)
		re, err := regexp.Compile(regexPattern)
		if err != nil {
			return nil, fmt.Errorf("invalid LIKE pattern: %v", err)
		}
		return func() (bool, error) {
			var value string
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return re.MatchString(value), nil
		}, nil
	case NotLike:
		pattern, ok := f.Value.(string)
		if !ok {
			return nil, fmt.Errorf("NOT LIKE operator requires a string pattern")
		}
		regexPattern := likeToRegex(pattern)
		re, err := regexp.Compile(regexPattern)
		if err != nil {
			return nil, fmt.Errorf("invalid NOT LIKE pattern: %v", err)
		}
		return func() (bool, error) {
			var value string
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return !re.MatchString(value), nil
		}, nil
	case In:
		values, err := extractStringSlice(f.Value)
		if err != nil {
			return nil, fmt.Errorf("operator %s requires a slice of strings: %v", f.Operator, err)
		}
		if len(values) == 0 {
			return nil, fmt.Errorf("operator %s requires a non-empty slice of values", f.Operator)
		}
		return func() (bool, error) {
			var value string
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return slices.Contains(values, value), nil
		}, nil
	case NotIn:
		values, err := extractStringSlice(f.Value)
		if err != nil {
			return nil, fmt.Errorf("operator %s requires a slice of strings: %v", f.Operator, err)
		}
		if len(values) == 0 {
			return nil, fmt.Errorf("operator %s requires a non-empty slice of values", f.Operator)
		}
		return func() (bool, error) {
			var value string
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return !slices.Contains(values, value), nil
		}, nil
	case IsNull:
		return func() (bool, error) {
			var value string
			ok, _ := f.scanFunc(&value)
			return !ok, nil
		}, nil
	case IsNotNull:
		return func() (bool, error) {
			var value string
			ok, _ := f.scanFunc(&value)
			return ok, nil
		}, nil
	case Between:
		minVal, maxVal, err := extractRangeString(f.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid range for BETWEEN operator: %v", err)
		}
		if minVal > maxVal {
			return nil, fmt.Errorf("invalid range for BETWEEN operator: min > max")
		}
		return func() (bool, error) {
			var value string
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value >= minVal && value <= maxVal, nil
		}, nil
	case NotBetween:
		minVal, maxVal, err := extractRangeString(f.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid range for NOT BETWEEN operator: %v", err)
		}
		if minVal > maxVal {
			return nil, fmt.Errorf("invalid range for NOT BETWEEN operator: min > max")
		}
		return func() (bool, error) {
			var value string
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value < minVal || value > maxVal, nil
		}, nil
	default:
		return nil, fmt.Errorf(errorUnsupportedOperatorString, f.Operator)
	}
}

func likeToRegex(pattern string) string {
	regexPattern := regexp.QuoteMeta(pattern)
	regexPattern = strings.ReplaceAll(regexPattern, "%", ".*")
	regexPattern = strings.ReplaceAll(regexPattern, "_", ".")
	return "^" + regexPattern + "$"
}

func extractStringSlice(value interface{}) ([]string, error) {
	raw, ok := value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("value must be slice of interface{}")
	}
	res := make([]string, len(raw))
	for i, v := range raw {
		vStr, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("value %v is not string", v)
		}
		res[i] = vStr
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
