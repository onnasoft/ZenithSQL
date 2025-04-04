package main

import (
	"fmt"
	"log"

	"github.com/onnasoft/ZenithSQL/model/catalog"
	"github.com/onnasoft/ZenithSQL/model/entity"
)

var tablename = "test"

func main() {
	db, err := initializeDatabase()
	if err != nil {
		log.Fatal(err)
	}

	schema, err := db.GetSchema("public")
	if err != nil {
		log.Fatal(err)
	}

	table, err := schema.GetTable(tablename)
	if err != nil {
		log.Fatal(err)
	}

	record, err := entity.NewEntity(table.Schema)
	if err != nil {
		log.Fatal(err)
	}

	record.SetByName("city", "bogota")
	record.SetByName("temperature", 25)

	/*
		err = table.Insert(record)
		if err != nil {
			log.Fatal(err)
		}*/

	record, err = table.Get(1)
	if err != nil {
		log.Fatal(err)
	}

	log.Println(record)
}

func initializeDatabase() (*catalog.Database, error) {
	db, err := catalog.NewDatabase("testdb", "./data")
	if err != nil {
		return nil, fmt.Errorf("creating database: %w", err)
	}

	schema, err := db.CreateSchema("public")
	if err != nil {
		return nil, fmt.Errorf("creating schema: %w", err)
	}

	fields := []*entity.Field{
		{
			Name:   "city",
			Type:   entity.StringType,
			Length: 100,
		},
		{
			Name:   "temperature",
			Type:   entity.Float64Type,
			Length: 8,
		},
		{
			Name: "record_time",
			Type: entity.TimestampType,
		},
	}

	_, err = schema.CreateTable(tablename, fields)
	if err != nil {
		return nil, fmt.Errorf("creating table: %w", err)
	}

	return db, nil
}
