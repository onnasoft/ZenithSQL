package statement

import "fmt"

type ColumnsDefinition []ColumnDefinition

func (c ColumnsDefinition) Len() int {
	return len(c)
}

func (c ColumnsDefinition) String() string {
	return fmt.Sprintf("%v", []ColumnDefinition(c))
}

type ColumnDefinition struct {
	Name         string `msgpack:"name" json:"name" valid:"required,alphanumunderscore"`
	Type         string `msgpack:"type" json:"type" valid:"required"`
	Length       int    `msgpack:"length" json:"length"`
	PrimaryKey   bool   `msgpack:"primary_key" json:"primary_key"`
	Index        bool   `msgpack:"index" json:"index"`
	DefaultValue string `msgpack:"default_value" json:"default_value"`
}
