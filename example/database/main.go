package main

import (
	"context"
	"encoding/json"

	"github.com/onnasoft/ZenithSQL/core/executor"
	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/onnasoft/ZenithSQL/io/filters"
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
		{"name": "Javier Xar", "email": "xarjavier@gmail.com", "age": int8(12)},
		{"name": "Jhon Doe", "email": "jhondoe@gmail.com", "age": int8(10)},
	}

	insertRecords(catalog, users)

	table, err := catalog.GetTable("testdb", "public", "users")
	if err != nil {
		log.Fatalf("error getting table: %v", err)
	}

	filter := filters.NewCondition("age", filters.Equal, int8(12))

	cursor, err := table.CursorWithFilter(filter)
	if err != nil {
		log.Fatalf("error creating cursor: %v", err)
	}
	defer cursor.Close()

	for cursor.Next() {
		record := map[string]interface{}{}
		err := cursor.Scan(record)
		if err != nil {
			log.Fatalf("error getting record: %v", err)
		}
		log.Infof("Record: %v", record)
	}
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
				Type:   types.String,
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
				Type:   types.String,
				Length: 100,
				Validators: []storage.ValidatorInfo{
					{
						Type:   "email",
						Params: json.RawMessage(`{}`),
					},
				},
			},
			{
				Name:       "age",
				Type:       types.Int8,
				Length:     8,
				Validators: []storage.ValidatorInfo{},
			},
		},
	}

	if catalog.ExistsTable("testdb", "public", "users") {
		log.Info("Table exists")
		if err = catalog.DropTable("testdb", "public", "users"); err != nil {
			log.Fatalf("error dropping table: %v", err)
		}
	}

	// Guardar configuración
	if _, err = schema.CreateTable("users", tableConfig); err != nil {
		log.Fatalf("error creating table: %v", err)
	}

	return catalog
}

func insertRecords(catalog *catalog.Catalog, users []map[string]interface{}) error {
	executor := executor.New(catalog)

	stmt, err := statement.NewInsertStatement("testdb", "public", "users", users)
	if err != nil {
		log.Fatal("Error creating insert statement: ", err)
	}

	if response := executor.Execute(context.Background(), stmt); !response.IsSuccess() {
		log.Fatal("Error executing insert statement: ", response.GetMessage())
	}

	return nil
}
