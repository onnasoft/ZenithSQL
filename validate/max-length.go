package validate

import "fmt"

type MaxLength struct {
	Limit int
}

func (v MaxLength) Validate(value interface{}, colName string) error {
	if str, ok := value.(string); ok && len(str) > v.Limit {
		return fmt.Errorf("column '%s' exceeds max length %d", colName, v.Limit)
	}
	return nil
}
