package main

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/onnasoft/ZenithSQL/io/filters"
	"github.com/onnasoft/ZenithSQL/model/catalog"
	"github.com/sirupsen/logrus"
)

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

	tests := []struct {
		name   string
		filter *filters.Filter
	}{
		{"Equal", filters.NewCondition("age", filters.Equal, int8(12))},
		{"NotEqual", filters.NewCondition("age", filters.NotEqual, int8(12))},
		{"GreaterThan", filters.NewCondition("age", filters.GreaterThan, int8(12))},
		{"GreaterThanOrEqual", filters.NewCondition("age", filters.GreaterThanOrEqual, int8(12))},
		{"LessThan", filters.NewCondition("age", filters.LessThan, int8(12))},
		{"LessThanOrEqual", filters.NewCondition("age", filters.LessThanOrEqual, int8(12))},
		{"In", filters.NewCondition("age", filters.In, []interface{}{int8(11), int8(12), int8(13)})},
		{"NotIn", filters.NewCondition("age", filters.NotIn, []interface{}{int8(11), int8(12), int8(13)})},
		{"IsNull", filters.NewCondition("age", filters.IsNull, nil)},
		{"IsNotNull", filters.NewCondition("age", filters.IsNotNull, nil)},
		{"Between", filters.NewCondition("age", filters.Between, []interface{}{int8(10), int8(15)})},
		{"NotBetween", filters.NewCondition("age", filters.NotBetween, []interface{}{int8(10), int8(15)})},
	}

	for _, test := range tests {
		test.filter.Prepare(cursor.Reader().ScanMap())
		ok, err := test.filter.Execute()
		if err != nil {
			fmt.Printf("[%s] Error: %v\n", test.name, err)
			continue
		}
		fmt.Printf("[%s] Result: %v\n", test.name, ok)
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
	defer catalog.Close()

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
