package main

import (
	"time"

	"github.com/onnasoft/ZenithSQL/dataframe"
	"github.com/onnasoft/ZenithSQL/entity"
	"github.com/onnasoft/ZenithSQL/validate"
	"github.com/sirupsen/logrus"
)

var log = logrus.New()

func main() {
	db, err := dataframe.NewDatabase("testdb", "./data")
	if err != nil {
		log.Fatal("Error creating database: ", err)
	}

	schema, err := db.CreateSchema("public")
	if err != nil {
		log.Fatal("Error creating schema: ", err)
	}
	table, err := schema.CreateTable("users")
	if err != nil {
		log.Fatal("Error creating table: ", err)
	}
	if err := table.AddColumn("name", entity.StringType, 10); err != nil {
		log.Fatal("Error adding column: ", err)
	}
	if err := table.AddColumn("email", entity.StringType, 20, validate.IsEmail{}); err != nil {
		log.Fatal("Error adding column: ", err)
	}

	log.Info("Table created successfully")
	log.Info("Table reserved: ", table.ReservedSize())

	users := []map[string]interface{}{
		{
			"name":  "Javier Xar",
			"email": "xarjavier@gmail.com",
		},
		{
			"name":  "Jhon Doe",
			"email": "jhondoe@gmail.com",
		},
	}

	records := make([]*entity.Entity, len(users))
	for user := range users {
		record, err := entity.NewEntity(table.Columns)
		if err != nil {
			log.Fatal("Error creating entity: ", err)
		}
		for key, value := range users[user] {
			if err := record.SetByName(key, value); err != nil {
				log.Fatal("Error setting value: ", err)
			}
		}

		now := time.Now()
		record.SetByName("created_at", now)
		record.SetByName("updated_at", now)
		record.SetByName("deleted_at", nil)

		records[user] = record
	}

	if err := table.Insert(records...); err != nil {
		log.Fatal("Error inserting records: ", err)
	}

	table.Print()

	columns := *table.Columns
	for i := int64(1); i <= table.Length(); i++ {
		record, err := entity.NewEntity(&columns)
		if err != nil {
			log.Fatal("Error creating row: ", err)
		}
		if err := table.Get(i, record); err != nil {
			log.Fatal("Error getting row: ", err)
		}
		log.Info(record.String())
	}
}
