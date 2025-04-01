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
	_, table := setupDatabaseAndTable()
	users := []map[string]interface{}{
		{"name": "Javier Xar", "email": "xarjavier@gmail.com"},
		{"name": "Jhon Doe", "email": "jhondoe@gmail.com"},
	}
	insertRecords(table, users)
	retrieveAndLogRecords(table)
}

func setupDatabaseAndTable() (*dataframe.Database, *dataframe.Table) {
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
	log.Info("Table reserved: ", table.EffectiveSize())

	return db, table
}

func insertRecords(table *dataframe.Table, users []map[string]interface{}) {
	records := make([]*entity.Entity, len(users))
	for i, user := range users {
		record, err := entity.NewEntity(table.Fields)
		if err != nil {
			log.Fatal("Error creating entity: ", err)
		}
		for key, value := range user {
			if err := record.SetByName(key, value); err != nil {
				log.Fatal("Error setting value: ", err)
			}
		}

		now := time.Now()
		record.SetByName("created_at", now)
		record.SetByName("updated_at", now)
		record.SetByName("deleted_at", nil)

		records[i] = record
	}

	if err := table.Insert(records...); err != nil {
		log.Fatal("Error inserting records: ", err)
	}
}

func retrieveAndLogRecords(table *dataframe.Table) {
	fields := table.Fields
	for i := int64(1); i <= table.Length(); i++ {
		record, err := entity.NewEntity(fields)
		if err != nil {
			log.Fatal("Error creating row: ", err)
		}
		if err := table.Get(i, record); err != nil {
			log.Fatal("Error getting row: ", err)
		}
		log.Info(record)
	}
}
