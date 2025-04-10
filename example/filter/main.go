package main

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/filters"
	"github.com/vmihailenco/msgpack/v5"
)

func main() {
	f := filters.NewGroup("OR").
		Add(
			filters.NewGroup("AND").
				Add(filters.NewCondition("age", filters.GreaterThan, 18)).
				Add(filters.NewCondition("status", filters.Equal, "active")),
		).
		Add(filters.NewCondition("country", filters.Equal, "US"))

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
}
