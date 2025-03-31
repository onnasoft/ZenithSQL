package main

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/dataframe"
)

func main() {
	db := dataframe.NewDatabase("testdb")
	schema := db.CreateSchema("public")
	table := schema.CreateTable("users").
		AddColumn("name", dataframe.StringType, 5).
		AddColumn("email", dataframe.StringType, 200)

	table.Insert("Jhon Doe", "jhondoe@gmail.com")
	table.Insert("Javier Xar", "xarjavier@gmail.com")
	fmt.Println(table.Insert("asdasss", "asdas"))

	fmt.Println("Table Name:", table.Name)
	fmt.Println("Table Columns:")
	for _, col := range table.Columns {
		fmt.Printf(" - %s (%s)\n", col.Name, col.Type.String())
	}

	table.Print()
}
