package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/onnasoft/ZenithSQL/core/executor"
	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/onnasoft/ZenithSQL/io/statement"
	"github.com/onnasoft/ZenithSQL/model/catalog"
	"github.com/onnasoft/ZenithSQL/model/types"
	"github.com/sirupsen/logrus"
)

var log = logrus.New()

func main() {
	catalog := setupDatabaseAndTable()
	defer catalog.Close()
	users := []map[string]interface{}{
		{"name": "Javier Xar", "email": "xarjavier@gmail.com", "avg": 1.2},
		{"name": "Jhon Doe", "email": "jhondoe@gmail.com", "avg": 2.3},
	}

	insertRecords(catalog, users)
	table, err := catalog.GetTable("testdb", "public", "users")
	if err != nil {
		log.Fatalf("error getting table: %v", err)
	}

	reader, err := table.Reader()
	if err != nil {
		log.Fatalf("error creating reader: %v", err)
	}
	defer reader.Close()

	reader.Seek(1)
	var name string
	reader.ReadValue("name", &name)
	fmt.Printf("name: %s\n", name)
	fmt.Println(reader.Values())
	fmt.Println(reader.Values()["avg"])
}

func setupDatabaseAndTable() *catalog.Catalog {
	catalog, err := catalog.OpenCatalog(&catalog.CatalogConfig{
		Path:   "./data",
		Logger: logrus.New(),
	})
	if err != nil {
		log.Fatalf("error opening catalog: %v", err)
	}
	defer catalog.Close()

	_, err = catalog.CreateDatabase("testdb")
	if err != nil {
		log.Fatalf("error creating database: %v", err)
	}

	db, err := catalog.GetDatabase("testdb")
	if err != nil {
		log.Fatalf("error opening database: %v", err)
	}

	_, err = db.CreateSchema("public")
	if err != nil {
		log.Fatalf("error creating schema: %v", err)
	}

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
			{
				Name:       "avg",
				Type:       types.Float64Type,
				Length:     8,
				Validators: []storage.ValidatorInfo{},
			},
		},
	}

	// Guardar configuración
	if _, err = schema.CreateTable("users", tableConfig); err == nil {
		err = schema.DropTable("users")
		if err != nil {
			log.Fatalf("error dropping table: %v", err)
		}
		log.Info("Table dropped successfully")
	}

	if _, err = schema.CreateTable("users", tableConfig); err != nil {
		log.Info("Table already exists, skipping creation")
	}

	return catalog
}

func insertRecords(catalog *catalog.Catalog, users []map[string]interface{}) error {
	executor := executor.New(catalog)

	stmt, err := statement.NewInsertStatement("testdb", "public", "users", users)
	if err != nil {
		log.Fatal("Error creating insert statement: ", err)
	}

	if _, err := executor.Execute(context.Background(), stmt); err != nil {
		log.Fatal("Error inserting records: ", err)
	}

	return nil
}

func retrieveAndLogRecords(table *catalog.Table) {
	fmt.Println("Retrieving records...")
}
