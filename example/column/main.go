package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/onnasoft/ZenithSQL/model/catalog"
	"github.com/onnasoft/ZenithSQL/model/fields"
	"github.com/sirupsen/logrus"
)

// Ejemplo de uso
func main() {
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
	fmt.Println("Schema path:", schema.Path)

	// Crear nueva tabla
	tableConfig := &storage.TableConfig{
		Fields: []fields.FieldMeta{
			{
				Name:   "name",
				Type:   fields.String,
				Length: 100,
				Validators: []fields.ValidatorInfo{
					{
						Type:   "stringLength",
						Params: json.RawMessage(`{"min": 1, "max": 100}`),
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
}
