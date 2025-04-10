package filters

import (
	"fmt"
	"strings"
)

func buildSimpleCondition(f *Filter) (string, []interface{}, error) {
	switch f.Operator {
	case IsNull, IsNotNull:
		return fmt.Sprintf("%s %s", f.Field, f.Operator), nil, nil
	case In, NotIn:
		slice, ok := f.Value.([]interface{})
		if !ok || len(slice) == 0 {
			return "", nil, fmt.Errorf("operator %s requires a non-empty slice", f.Operator)
		}
		placeholders := strings.Repeat("?, ", len(slice))
		placeholders = placeholders[:len(placeholders)-2]
		return fmt.Sprintf("%s %s (%s)", f.Field, f.Operator, placeholders), slice, nil
	case Between, NotBetween:
		rangeVals, ok := f.Value.([]interface{})
		if !ok || len(rangeVals) != 2 {
			return "", nil, fmt.Errorf("operator %s requires exactly 2 values", f.Operator)
		}
		return fmt.Sprintf("%s %s ? AND ?", f.Field, f.Operator), rangeVals, nil
	default:
		return fmt.Sprintf("%s %s ?", f.Field, f.Operator), []interface{}{f.Value}, nil
	}
}
