package validate

import (
	"errors"
	"strings"
)

type Validator interface {
	FromMap(map[string]interface{})
	ToMap() map[string]interface{}
	Validate(value interface{}, colName string) error
	Type() string
}

type Validators []Validator

func (v Validators) String() string {
	var sb strings.Builder

	for i := 0; i < len(v); i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(v[i].Type())
	}

	return sb.String()
}

func (v Validators) Len() int {
	return len(v)
}

func (v Validators) ToMap() []map[string]interface{} {
	validators := make([]map[string]interface{}, v.Len())
	for i := 0; i < v.Len(); i++ {
		validators[i] = v[i].ToMap()
	}
	return validators
}

var fromMapFunc = map[string]func() Validator{
	IsEmail{}.Type(): func() Validator {
		return &IsEmail{}
	},
	StringLength{}.Type(): func() Validator {
		return &StringLength{}
	},
}

func FromMap(value map[string]interface{}) (Validator, error) {
	var result Validator
	if generator, ok := fromMapFunc[value["type"].(string)]; ok {
		result = generator()
		result.FromMap(value)
	} else {
		return nil, errors.New("Validator does't exists")
	}

	return result, nil
}
