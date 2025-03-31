package validate

import (
	"fmt"
)

type InRange struct {
	Min interface{}
	Max interface{}
}

func (v InRange) Validate(value interface{}, colName string) error {
	switch val := value.(type) {
	case int:
		min, ok1 := v.Min.(int)
		max, ok2 := v.Max.(int)
		if ok1 && ok2 && (val >= min && val <= max) {
			return nil
		}
	case int64:
		min, ok1 := v.Min.(int64)
		max, ok2 := v.Max.(int64)
		if ok1 && ok2 && (val >= min && val <= max) {
			return nil
		}
	case float64:
		min, ok1 := v.Min.(float64)
		max, ok2 := v.Max.(float64)
		if ok1 && ok2 && (val >= min && val <= max) {
			return nil
		}
	}
	return fmt.Errorf("column '%s' must be in range [%v, %v]", colName, v.Min, v.Max)
}
