package main

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/core/buffer"
	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/onnasoft/ZenithSQL/io/filters"
	"github.com/onnasoft/ZenithSQL/model/catalog"
	"github.com/onnasoft/ZenithSQL/model/fields"
	"github.com/sirupsen/logrus"
)

type Filters struct {
	Scanner buffer.Scanner
	filter  []Filter
}

type Filter struct {
	name   string
	filter *filters.Filter
	expect bool
}

func main() {
	catalog, table, cursor, err := setup()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer catalog.Close()
	defer table.Close()
	defer cursor.Close()

	if !cursor.Next() {
		fmt.Println("No rows to test")
		return
	}

	tests := []Filters{
		{
			Scanner: buffer.Scanner{
				Type: fields.Int8Type{},
				Scan: func(value interface{}) (bool, error) {
					if v, ok := value.(*int8); ok {
						*v = int8(12)
					} else {
						return false, fmt.Errorf("value is not of type *int8")
					}

					return true, nil
				},
				Nullable: false,
			},
			filter: []Filter{
				{"Equal", filters.NewCondition("age", filters.Equal, int8(12)), true},
				{"NotEqual", filters.NewCondition("age", filters.NotEqual, int8(12)), false},
				{"GreaterThan", filters.NewCondition("age", filters.GreaterThan, int8(12)), false},
				{"GreaterThanOrEqual", filters.NewCondition("age", filters.GreaterThanOrEqual, int8(12)), true},
				{"LessThan", filters.NewCondition("age", filters.LessThan, int8(12)), false},
				{"LessThanOrEqual", filters.NewCondition("age", filters.LessThanOrEqual, int8(12)), true},
				{"In", filters.NewCondition("age", filters.In, []interface{}{int8(11), int8(12), int8(13)}), true},
				{"NotIn", filters.NewCondition("age", filters.NotIn, []interface{}{int8(11), int8(12), int8(13)}), false},
				{"IsNull", filters.NewCondition("age", filters.IsNull, nil), false},
				{"IsNotNull", filters.NewCondition("age", filters.IsNotNull, nil), true},
				{"Between", filters.NewCondition("age", filters.Between, []interface{}{int8(10), int8(15)}), true},
				{"NotBetween", filters.NewCondition("age", filters.NotBetween, []interface{}{int8(10), int8(15)}), false},
			},
		},
	}

	for _, filter := range tests {
		for _, test := range filter.filter {
			test.filter.Prepare(map[string]*buffer.Scanner{
				"age": &filter.Scanner,
			})

			ok, err := test.filter.Execute()
			if err != nil {
				fmt.Printf("[%s] Error: %v\n", test.name, err)
				continue
			}

			if test.expect != ok {
				fmt.Printf("[%s] Expected: %v, got: %v\n", test.name, test.expect, ok)
			} else {
				fmt.Printf("[%s] Passed\n", test.name)
			}
		}
	}
}

func setup() (*catalog.Catalog, *catalog.Table, storage.Cursor, error) {
	catalog, err := catalog.OpenCatalog(&catalog.CatalogConfig{
		Path:   "./data",
		Logger: logrus.New(),
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to open catalog: %w", err)
	}

	table, err := catalog.GetTable("testdb", "public", "users")
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get table: %w", err)
	}

	cursor, err := table.Cursor()
	if err != nil {
		fmt.Println("Error:", err)
		return nil, nil, nil, fmt.Errorf("failed to create cursor: %w", err)
	}

	return catalog, table, cursor, nil
}
