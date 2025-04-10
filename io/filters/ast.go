package filters

import (
	"errors"
	"strings"

	"github.com/onnasoft/ZenithSQL/core/storage"
)

type operator string

const (
	Equal              operator = "="
	NotEqual           operator = "!="
	GreaterThan        operator = ">"
	GreaterThanOrEqual operator = ">="
	LessThan           operator = "<"
	LessThanOrEqual    operator = "<="
	Like               operator = "LIKE"
	NotLike            operator = "NOT LIKE"
	In                 operator = "IN"
	NotIn              operator = "NOT IN"
	IsNull             operator = "IS NULL"
	IsNotNull          operator = "IS NOT NULL"
	Between            operator = "BETWEEN"
	NotBetween         operator = "NOT BETWEEN"
)

type Filter struct {
	// Nodo hoja (condición simple)
	Database string
	Schema   string
	Table    string
	Field    string
	Operator operator
	Value    interface{}
	scanFunc storage.ScanFunc
	filter   filterFn

	// Nodo compuesto (agrupación lógica)
	JoinWith string    // "AND", "OR"
	Children []*Filter // subcondiciones agrupadas
}

func NewGroup(joinWith string) *Filter {
	return &Filter{
		JoinWith: joinWith,
		Children: []*Filter{},
	}
}

func NewCondition(field string, op operator, value interface{}) *Filter {
	return &Filter{
		Field:    field,
		Operator: op,
		Value:    value,
	}
}

func (f *Filter) Add(child *Filter) *Filter {
	f.Children = append(f.Children, child)
	return f
}

func (f *Filter) Build() (string, []interface{}, error) {
	if f.Field != "" && f.Operator != "" {
		return buildSimpleCondition(f)
	}

	if len(f.Children) == 0 {
		return "", nil, errors.New("empty filter group")
	}

	var parts []string
	var values []interface{}

	for _, child := range f.Children {
		part, val, err := child.Build()
		if err != nil {
			return "", nil, err
		}
		parts = append(parts, "("+part+")")
		values = append(values, val...)
	}

	return strings.Join(parts, " "+f.JoinWith+" "), values, nil
}

func (f *Filter) Prepare(scanMap map[string]*storage.ColumnScanner) error {
	if len(f.Children) == 0 {
		columnData, ok := scanMap[f.Field]
		if !ok {
			return errors.New("field not found")
		}

		f.scanFunc = columnData.Scan

		filter, ok := mapEqOps[columnData.Type]
		if !ok {
			return errors.New("unsupported type")
		}
		fn, err := filter(f)
		if err != nil {
			return err
		}
		f.filter = fn

		return nil
	}

	for _, child := range f.Children {
		if err := child.Prepare(scanMap); err != nil {
			return err
		}
	}

	return nil
}

func (f *Filter) Execute() (bool, error) {
	if f.filter == nil {
		return false, errors.New("filter not prepared")
	}
	return f.filter()
}
