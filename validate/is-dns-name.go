package validate

import (
	"fmt"
	"regexp"
)

type IsDNSName struct{}

var dnsNameRegex = regexp.MustCompile(`^(?i:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)(?:\.(?i:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?))*$`)

func (v IsDNSName) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok || !dnsNameRegex.MatchString(str) {
		return fmt.Errorf("column '%s' must be a valid DNS name", colName)
	}
	return nil
}
