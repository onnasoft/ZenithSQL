package main

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/dataframe"
	"github.com/onnasoft/ZenithSQL/validate"
	"github.com/sirupsen/logrus"
)

var log = logrus.New()

func main() {
	db, err := dataframe.NewDatabase("testdb", "./data")
	if err != nil {
		log.Fatal(err)
	}

	schema, err := db.CreateSchema("public")
	if err != nil {
		log.Fatal(err)
	}
	table, err := schema.CreateTable("users")
	if err != nil {
		log.Fatal(err)
	}
	if err := table.AddColumn("name", dataframe.StringType, 10); err != nil {
		log.Fatal(err)
	}
	if err := table.AddColumn("email", dataframe.StringType, 20, validate.IsEmail{}); err != nil {
		log.Fatal(err)
	}

	log.Info("Table created successfully")
	log.Info("Table reserved: ", table.ReservedSize())

	if err := table.Insert("Jhon Doe", "jhondoe@gmail.com"); err != nil {
		log.Fatal(err)
	}
	if err := table.Insert("Javier Xar", "xarjavier@gmail.com"); err != nil {
		log.Fatal(err)
	}

	table.Print()

	for i := int64(1); i <= table.Length(); i++ {
		row, err := table.Get(i, &dataframe.Columns{
			table.Columns.Get(0),
		})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(row)
	}
}
