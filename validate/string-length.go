package validate

import "fmt"

type StringLengthValidator struct {
	Min int
	Max int
}

func (v StringLengthValidator) Validate(value interface{}, colName string) error {
	if str, ok := value.(string); ok {
		if len(str) < v.Min || len(str) > v.Max {
			return fmt.Errorf("column '%s' must be between %d and %d characters", colName, v.Min, v.Max)
		}
	}
	return nil
}

func (v StringLengthValidator) String() string {
	return fmt.Sprintf("stringLength(%v, %v)", v.Min, v.Max)
}
