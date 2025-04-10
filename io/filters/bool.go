package filters

import (
	"fmt"
)

const errorUnsupportedOperatorBool = "unsupported operator %s for type bool"

func filterBool(f *Filter) (filterFn, error) {
	switch f.Operator {
	case Equal:
		data, ok := f.Value.(bool)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorBool, f.Operator)
		}
		return func() (bool, error) {
			var value bool
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value == data, nil
		}, nil
	case NotEqual:
		data, ok := f.Value.(bool)
		if !ok {
			return nil, fmt.Errorf(errorUnsupportedOperatorBool, f.Operator)
		}
		return func() (bool, error) {
			var value bool
			if _, err := f.scanFunc(&value); err != nil {
				return false, err
			}
			return value != data, nil
		}, nil
	case IsNull:
		return func() (bool, error) {
			var value bool
			ok, _ := f.scanFunc(&value)
			return !ok, nil
		}, nil
	case IsNotNull:
		return func() (bool, error) {
			var value bool
			ok, _ := f.scanFunc(&value)
			return ok, nil
		}, nil
	// Operadores no aplicables para bool
	case GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Like, NotLike, In, NotIn, Between, NotBetween:
		return func() (bool, error) {
			return false, fmt.Errorf("operator %s is not applicable for boolean values", f.Operator)
		}, nil
	default:
		return nil, fmt.Errorf(errorUnsupportedOperatorBool, f.Operator)
	}
}
