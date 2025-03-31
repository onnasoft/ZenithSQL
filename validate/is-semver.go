package validate

import (
	"fmt"
	"regexp"
)

type IsSemver struct{}

var semverRegex = regexp.MustCompile(`^v?(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(-[\w\d-.]+)?(\+[\w\d-.]+)?$`)

func (v IsSemver) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok || !semverRegex.MatchString(str) {
		return fmt.Errorf("column '%s' must be a valid semantic version (e.g., 1.2.3)", colName)
	}
	return nil
}
