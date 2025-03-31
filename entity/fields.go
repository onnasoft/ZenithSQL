package entity

import (
	"errors"
	"fmt"

	"github.com/onnasoft/ZenithSQL/validate"
)

const (
	errIndexOutOfRange = "index out of range"
)

type Field struct {
	Name          string
	Type          DataType
	Length        int
	Validators    []validate.Validator
	StartPosition int
	EndPosition   int
	NullFlagPos   int
}

type Fields []Field

func (f Fields) Len() int {
	return len(f)
}

func (f Fields) Get(index int) (Field, error) {
	if index < 0 || index >= len(f) {
		return Field{}, errors.New(errIndexOutOfRange)
	}
	return f[index], nil
}

func (f *Fields) Insert(index int, field Field) error {
	if index < 0 || index > len(*f) {
		return errors.New(errIndexOutOfRange)
	}
	*f = append(*f, Field{})
	copy((*f)[index+1:], (*f)[index:])
	(*f)[index] = field
	return nil
}

func (f *Fields) Add(field Field) {
	*f = append(*f, field)
}

func (f *Fields) Remove(index int) error {
	if index < 0 || index >= len(*f) {
		return errors.New(errIndexOutOfRange)
	}
	copy((*f)[index:], (*f)[index+1:])
	*f = (*f)[:len(*f)-1]
	return nil
}

func (f Fields) String() string {
	var result string
	for _, field := range f {
		result += fmt.Sprintf("%s (%s), ", field.Name, field.Type.String())
	}
	return result
}
