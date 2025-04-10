package filters

import (
	"fmt"
)

const errorUnsupportedOperatorBool = "unsupported operator %s for type bool"

func filterBool(f *Filter) (filterFn, error) {
	switch f.Operator {
	case Equal:
		return compareBool(f, func(a, b bool) bool { return a == b })
	case NotEqual:
		return compareBool(f, func(a, b bool) bool { return a != b })
	case IsNull:
		return isNullBool(f, true)
	case IsNotNull:
		return isNullBool(f, false)
	default:
		return func() (bool, error) {
			return false, fmt.Errorf(errorUnsupportedOperatorBool, f.Operator)
		}, nil
	}
}

func compareBool(f *Filter, cmp func(a, b bool) bool) (filterFn, error) {
	data, ok := f.Value.(bool)
	if !ok {
		return nil, fmt.Errorf(errorUnsupportedOperatorBool, f.Operator)
	}
	return func() (bool, error) {
		var value bool
		if _, err := f.scanFunc(&value); err != nil {
			return false, err
		}
		return cmp(value, data), nil
	}, nil
}

func isNullBool(f *Filter, expectNull bool) (filterFn, error) {
	return func() (bool, error) {
		var value bool
		ok, _ := f.scanFunc(&value)
		return expectNull != ok, nil
	}, nil
}
