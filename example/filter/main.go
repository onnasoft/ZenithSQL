package main

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/filters"
	"github.com/onnasoft/ZenithSQL/model/catalog"
	"github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack/v5"
)

func main() {
	f := filters.NewCondition("age", filters.Equal, int8(12))

	sql, values, err := f.Build()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Println("SQL:", sql)
	fmt.Println("Values:", values)

	msg, _ := msgpack.Marshal(f)
	var f2 filters.Filter
	err = msgpack.Unmarshal(msg, &f2)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	sql2, values2, err := f2.Build()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("SQL2:", sql2)
	fmt.Println("Values2:", values2)

	catalog, err := catalog.OpenCatalog(&catalog.CatalogConfig{
		Path:   "./data",
		Logger: logrus.New(),
	})
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer catalog.Close()

	table, err := catalog.GetTable("testdb", "public", "users")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	cursor, err := table.Cursor()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer cursor.Close()

	f2.Prepare(table.ColumnsData(), cursor)
	cursor.Next()

	ok, err := f2.Execute()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	if ok {
		fmt.Println("Condition met")
	} else {
		fmt.Println("Condition not met")
	}
}
