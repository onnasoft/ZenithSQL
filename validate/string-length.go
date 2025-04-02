package validate

import "fmt"

type StringLength struct {
	Min int
	Max int
}

func (v StringLength) Validate(value interface{}, colName string) error {
	if str, ok := value.(string); ok {
		if len(str) < v.Min || len(str) > v.Max {
			return fmt.Errorf("column '%s' must be between %d and %d characters", colName, v.Min, v.Max)
		}
	}
	return nil
}

func (v StringLength) Type() string {
	return "stringLength"
}

func (v *StringLength) FromMap(value map[string]interface{}) {
	v.Min = int(value["min"].(float64))
	v.Max = int(value["max"].(float64))
}

func (v StringLength) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"type": "stringLength",
		"min":  v.Min,
		"max":  v.Max,
	}
}
