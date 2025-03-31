package dataframe

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/validate"
)

type Column struct {
	Name          string
	Type          DataType
	Length        int
	Validators    []validate.Validator
	StartPosition int
	EndPosition   int
	NullFlagPos   int
}

type Columns []Column

func (c Columns) Len() int {
	return len(c)
}

func (c Columns) Get(index int) Column {
	if index < 0 || index >= len(c) {
		panic("index out of range")
	}
	return c[index]
}

func (c *Columns) Insert(index int, col Column) {
	if index < 0 || index > len(*c) {
		panic("index out of range")
	}
	*c = append(*c, Column{})
	copy((*c)[index+1:], (*c)[index:])
	(*c)[index] = col
}

func (c *Columns) Add(col Column) {
	*c = append(*c, col)
}

func (c *Columns) Remove(index int) {
	if index < 0 || index >= len(*c) {
		panic("index out of range")
	}
	copy((*c)[index:], (*c)[index+1:])
	*c = (*c)[:len(*c)-1]
}

func (c Columns) String() string {
	var result string
	for _, col := range c {
		result += fmt.Sprintf("%s (%s), ", col.Name, col.Type.String())
	}
	return result
}
