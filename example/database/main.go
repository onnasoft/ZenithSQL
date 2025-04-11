package main

import (
	"context"
	"encoding/json"

	"github.com/onnasoft/ZenithSQL/core/executor"
	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/onnasoft/ZenithSQL/io/filters"
	"github.com/onnasoft/ZenithSQL/io/response"
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

	filter := filters.NewCondition("age", filters.Equal, int8(12))

	stmt, err := statement.NewSelectStatement(statement.SelectStatementConfig{
		Database:  "testdb",
		Schema:    "public",
		TableName: "users",
		Where:     filter,
		Columns:   []string{"name", "email"},
	})
	if err != nil {
		log.Fatalf("error creating select statement: %v", err)
	}

	executor := executor.New(catalog)

	result := executor.Execute(context.Background(), stmt)
	if !result.IsSuccess() {
		log.Fatalf("error executing select statement: %v", result.GetMessage())
	}

	response, ok := result.(*response.SelectResponse)
	if !ok {
		log.Fatalf("error casting response to SelectResponse")
	}

	log.Infof("Select executed successfully: %v", response.Rows)
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
