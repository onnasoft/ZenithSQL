package validate

import (
	"fmt"
	"regexp"
)

type IsMongoID struct{}

var mongoIDPattern = regexp.MustCompile(`^[a-fA-F0-9]{24}$`)

func (v IsMongoID) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok || !mongoIDPattern.MatchString(str) {
		return fmt.Errorf("column '%s' must be a valid MongoID", colName)
	}
	return nil
}
