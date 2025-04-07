package main

import (
	"encoding/json"
	"fmt"

	"github.com/onnasoft/ZenithSQL/core/executor"
	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/onnasoft/ZenithSQL/model/catalog"
	"github.com/onnasoft/ZenithSQL/model/types"
	"github.com/sirupsen/logrus"
)

var log = logrus.New()

func main() {
	db, table := setupDatabaseAndTable()
	defer db.Close()
	users := []map[string]interface{}{
		{"name": "Javier Xar", "email": "xarjavier@gmail.com"},
		{"name": "Jhon Doe", "email": "jhondoe@gmail.com"},
	}
	insertRecords(table, users)
	retrieveAndLogRecords(table)

}

func setupDatabaseAndTable() (*catalog.Database, *catalog.Table) {
	db, err := catalog.OpenDatabase(&catalog.DatabaseConfig{
		Name:   "testdb",
		Path:   "./data",
		Logger: logrus.New(),
	})
	if err != nil {
		log.Fatalf("error opening database: %v", err)
	}
	defer db.Close()

	schema, err := db.GetSchema("public")
	if err != nil {
		log.Fatalf("error getting schema: %v", err)
	}

	// Crear nueva tabla
	tableConfig := &storage.TableConfig{
		Fields: []storage.FieldMeta{
			{
				Name:   "name",
				Type:   types.StringType,
				Length: 100,
				Validators: []storage.ValidatorInfo{
					{
						Type:   "stringLength",
						Params: json.RawMessage(`{"min": 1, "max": 100}`),
					},
				},
			},
			{
				Name:   "email",
				Type:   types.StringType,
				Length: 100,
				Validators: []storage.ValidatorInfo{
					{
						Type:   "email",
						Params: json.RawMessage(`{}`),
					},
				},
			},
		},
	}

	// Guardar configuración
	table, err := schema.CreateTable("users", tableConfig)
	if err != nil {
		log.Fatalf("error creating table: %v", err)
	}
	fmt.Println("Table created:", table.Name)

	return db, table
}

func insertRecords(table *catalog.Table, users []map[string]interface{}) {
	if err := executor.Insert(table, users...); err != nil {
		log.Fatal("Error inserting records: ", err)
	}
}

func retrieveAndLogRecords(table *catalog.Table) {
	fmt.Println("Retrieving records...")
}
