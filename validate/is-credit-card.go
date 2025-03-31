package validate

import (
	"fmt"
	"regexp"
	"strings"
)

type IsCreditCard struct{}

func (v IsCreditCard) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsCreditCard validation", colName)
	}
	str = strings.ReplaceAll(str, "-", "")
	str = strings.ReplaceAll(str, " ", "")
	if !regexp.MustCompile(`^\d{13,19}$`).MatchString(str) {
		return fmt.Errorf("column '%s' is not a valid credit card number", colName)
	}
	var sum int
	alt := false
	for i := len(str) - 1; i > -1; i-- {
		n := int(str[i] - '0')
		if alt {
			n *= 2
			if n > 9 {
				n -= 9
			}
		}
		sum += n
		alt = !alt
	}
	if sum%10 != 0 {
		return fmt.Errorf("column '%s' failed Luhn check", colName)
	}
	return nil
}
