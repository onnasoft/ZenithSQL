package validate

import (
	"strings"
)

type Validator interface {
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

var MakeValidator = map[string]func() Validator{
	IsEmail{}.Type(): func() Validator {
		return &IsEmail{}
	},
	StringLength{}.Type(): func() Validator {
		return &StringLength{}
	},
}
