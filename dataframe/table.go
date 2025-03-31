package dataframe

import (
	"fmt"
	"slices"
	"time"

	"github.com/onnasoft/ZenithSQL/validate"
)

type DataType int

const (
	Int64Type DataType = iota
	Float64Type
	StringType
	BoolType
	TimestampType
)

func (dt DataType) String() string {
	switch dt {
	case Int64Type:
		return "int64"
	case Float64Type:
		return "float64"
	case StringType:
		return "string"
	case BoolType:
		return "bool"
	case TimestampType:
		return "timestamp"
	default:
		return "unknown"
	}
}

type Table struct {
	Name    string
	Columns []Column
	Rows    [][]interface{}
}

func NewTable(name string) *Table {
	t := &Table{Name: name}
	t.Columns = []Column{
		{Name: "id", Type: Int64Type},
		{Name: "created_at", Type: TimestampType},
		{Name: "updated_at", Type: TimestampType},
		{Name: "deleted_at", Type: TimestampType},
	}
	return t
}

func (t *Table) AddColumn(name string, typ DataType, length int, validators ...validate.Validator) *Table {
	if length == 0 {
		switch typ {
		case Int64Type, Float64Type, TimestampType:
			length = 8
		case BoolType:
			length = 1
		}
	}

	col := Column{
		Name:   name,
		Type:   typ,
		Length: length,
	}
	if typ == StringType {
		col.Validators = append(col.Validators, validate.StringLengthValidator{Min: 0, Max: length})
	} else {
		col.Validators = validators
	}

	t.Columns = slices.Insert(t.Columns, len(t.Columns)-3, col)
	return t
}

func (t *Table) Insert(values ...interface{}) error {
	userColumns := len(t.Columns) - 4
	if len(values) != userColumns {
		return fmt.Errorf("expected %d values, got %d", userColumns, len(values))
	}

	row := make([]interface{}, 0, len(t.Columns))
	row = append(row, int64(len(t.Rows)+1))

	for i, val := range values {
		col := t.Columns[i+1]

		if !isValidType(col.Type, val) {
			return fmt.Errorf("column '%s' expects %s, got %T", col.Name, col.Type.String(), val)
		}

		for _, validator := range col.Validators {
			if err := validator.Validate(val, col.Name); err != nil {
				return err
			}
		}

		row = append(row, val)
	}

	now := time.Now().UnixNano()
	row = append(row, now, now, nil)

	t.Rows = append(t.Rows, row)
	return nil
}

var printFuncs = map[DataType]func(interface{}) string{
	Int64Type: func(v interface{}) string {
		return fmt.Sprintf("%d", v.(int64))
	},
	Float64Type: func(v interface{}) string {
		return fmt.Sprintf("%.2f", v.(float64))
	},
	StringType: func(v interface{}) string {
		return fmt.Sprintf("%s", v)
	},
	BoolType: func(v interface{}) string {
		return fmt.Sprintf("%t", v.(bool))
	},
	TimestampType: func(v interface{}) string {
		timestamp := time.Unix(0, v.(int64))
		return timestamp.Format(time.RFC3339)
	},
}

func (t *Table) PrintRow(row []interface{}) {
	for i, col := range t.Columns {
		if row[i] == nil {
			fmt.Printf("%s=NULL ", col.Name)
			continue
		}
		printFunc, ok := printFuncs[col.Type]
		if !ok {
			fmt.Printf("%s=%v ", col.Name, row[i])
			continue
		}
		fmt.Printf("%s=%s ", col.Name, printFunc(row[i]))
	}
	fmt.Println()
}

func (t *Table) Print() {
	for _, row := range t.Rows {
		fmt.Printf("Row: ")
		t.PrintRow(row)
	}
}

func (s *Schema) CreateTable(name string) *Table {
	t := NewTable(name)
	s.Tables[name] = t
	return t
}
