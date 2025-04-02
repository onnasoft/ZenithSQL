package main

import (
	"time"

	"github.com/onnasoft/ZenithSQL/engine"
	"github.com/onnasoft/ZenithSQL/entity"
	"github.com/onnasoft/ZenithSQL/utils"
	"github.com/onnasoft/ZenithSQL/validate"
	"github.com/sirupsen/logrus"
)

var log = logrus.New()

func main() {
	utils.Clone(entity.Field{})
	utils.Clone(&entity.Field{})

	db, table := setupDatabaseAndTable()
	defer db.Close()
	users := []map[string]interface{}{
		{"name": "Javier Xar", "email": "xarjavier@gmail.com"},
		{"name": "Jhon Doe", "email": "jhondoe@gmail.com"},
	}
	insertRecords(table, users)
	retrieveAndLogRecords(table)

	schema, err := db.GetSchema("public")
	if err != nil {
		log.Fatal("Error getting schema: ", err)
	}

	table, err = schema.LoadTable("users")
	if err != nil {
		log.Fatal("Error loading table: ", err)
	}
}

func setupDatabaseAndTable() (*engine.Database, *engine.Table) {
	db, err := engine.NewDatabase("testdb", "./data")
	if err != nil {
		log.Fatal("Error creating database: ", err)
	}

	schema, err := db.CreateSchema("public")
	if err != nil {
		log.Fatal("Error creating schema: ", err)
	}
	fields := []*entity.Field{
		{
			Name:   "name",
			Type:   entity.StringType,
			Length: 100,
		},
		{
			Name:   "email",
			Type:   entity.StringType,
			Length: 100,
			Validators: []validate.Validator{
				validate.IsEmail{},
			},
		},
	}
	table, err := schema.CreateTable("users", fields)
	if err != nil {
		log.Fatal("Error creating table: ", err)
	}

	log.Info("Table created successfully")
	log.Info("Table reserved: ", table.EffectiveSize())

	return db, table
}

func insertRecords(table *engine.Table, users []map[string]interface{}) {
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

func retrieveAndLogRecords(table *engine.Table) {
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
