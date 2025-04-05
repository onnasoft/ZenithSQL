package main

import (
	"fmt"
	"log"

	"github.com/onnasoft/ZenithSQL/core/executor"
	"github.com/onnasoft/ZenithSQL/model/catalog"
	"github.com/onnasoft/ZenithSQL/model/entity"
	"github.com/sirupsen/logrus"
)

func main() {
	db, err := catalog.OpenDatabase(&catalog.DatabaseConfig{
		Name:   "testdb",
		Path:   "./data",
		Logger: logrus.New(),
	})
	if err != nil {
		log.Fatalf("error getting database %v, %v", "testdb", err)
	}

	schema, err := db.GetSchema("public")
	if err != nil {
		log.Fatalf("error getting schema %v, %v", "public", err)
	}
	sc := entity.NewSchema()
	sc.AddField(catalog.NewFieldString("city", 100))
	sc.AddField(catalog.NewFieldFloat64("temperature"))

	table, err := schema.CreateTable("temperatures", sc)
	if err != nil {
		log.Fatalf("error getting table %v, %v", "temperatures", err)
	}
	fmt.Println("Table Name:", table.Name)

	for _, field := range sc.Iter() {
		fmt.Printf("Field: %s, Type: %s, Length: %d\n", field.Name, field.Type.String(), field.Length)
	}

	fmt.Println("Schema Size:", sc.CalculateSize())
	fmt.Println("Schema Length:", sc.Len())
	fmt.Println("Schema Size:", sc.Size())

	fmt.Println()
	record := table.NewRow()
	if err := record.Data.SetValue("city", "New York"); err != nil {
		log.Fatalf("error setting value %v", err)
	}
	if err := record.Data.SetValue("temperature", 25.5); err != nil {
		log.Fatalf("error setting value %v", err)
	}

	fmt.Println()
	executor.Insert(table, record)

	fmt.Println("Record", record)
}
