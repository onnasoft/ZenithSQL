package statement

import (
	"regexp"

	"github.com/asaskevich/govalidator"
)

func init() {
	govalidator.TagMap["alphanumunderscore"] = govalidator.Validator(func(str string) bool {
		match, _ := regexp.MatchString(`^[a-zA-Z_][a-zA-Z0-9_]*$`, str)
		return match
	})
}
