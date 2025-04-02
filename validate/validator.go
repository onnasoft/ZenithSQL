package validate

import (
	"strings"
)

type Validator interface {
	Validate(value interface{}, colName string) error
	String() string
}

type Validators []Validator

func (v Validators) String() string {
	var sb strings.Builder

	for i := 0; i < len(v); i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(v[i].String())
	}

	return sb.String()
}

func (v Validators) Len() int {
	return len(v)
}
