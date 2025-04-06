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

	fmt.Println("Schema Length:", sc.Len())
	fmt.Println("Schema Size:", sc.Size())

	fmt.Println()
	values := []interface{}{
		map[string]interface{}{
			"city":        "New York",
			"temperature": 25.5,
		},
		map[string]interface{}{
			"city":        "Los Angeles",
			"temperature": 30.0,
		},
		map[string]interface{}{
			"city":        "Chicago",
			"temperature": 20.0,
		},
	}
	for _, v := range values {
		record := table.NewRow()
		if err := record.Data.SetValue("city", v.(map[string]interface{})["city"]); err != nil {
			log.Fatalf("error setting value %v", err)
		}
		if err := record.Data.SetValue("temperature", v.(map[string]interface{})["temperature"]); err != nil {
			log.Fatalf("error setting value %v", err)
		}
		if err := executor.Insert(table, record); err != nil {
			log.Fatalf("error inserting record %v", err)
		}
	}
	fmt.Println("Inserted records:")

	for i := 0; i < len(values); i++ {
		row := table.LoadRow(uint64(i + 1))
		if row == nil {
			log.Fatalf("error getting record %v", i+1)
		}
		row.Data.Reset()
		fmt.Printf("Row ID: %d, City: %s, Temperature: %f\n", row.GetID(), row.Data.GetValue("city"), row.Data.GetValue("temperature"))
	}
	fmt.Println("Total Rows:", table.Stats.GetValue("rows"))
	fmt.Println("Row Size:", table.Stats.GetValue("row_size"))
}
