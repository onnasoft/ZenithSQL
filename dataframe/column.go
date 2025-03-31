package dataframe

import "github.com/onnasoft/ZenithSQL/validate"

type Column struct {
	Name       string
	Type       DataType
	Length     int
	Validators []validate.Validator
}
