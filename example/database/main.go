package main

import (
	"github.com/onnasoft/ZenithSQL/io/filters"
	"github.com/onnasoft/ZenithSQL/io/statement"
	"github.com/sirupsen/logrus"
)

var log = logrus.New()

func main() {
	catalog := setupDatabaseAndTable()
	defer catalog.Close()
	users := []map[string]interface{}{
		{
			"name":    "Javier Xar",
			"email":   "xarjavier@gmail.com",
			"country": "Spain",
			"age":     int8(12),
		},
		{
			"name":    "Jhon Doe",
			"email":   "jhondoe@gmail.com",
			"country": "Spain",
			"age":     int8(10),
		},
	}

	insertRecords(catalog, users)

	stmt, err := statement.NewSelectStatement(statement.SelectStatementConfig{
		Database:  "testdb",
		Schema:    "public",
		TableName: "users",
		Columns:   []string{"country"},
		Aggregations: []statement.Aggregation{
			{
				Function: "COUNT",
				Column:   "country",
				Alias:    "count",
			},
		},
		GroupBy: []string{"name"},
		Where:   filters.NewCondition("age", filters.Equal, int8(12)),
	})
	if err != nil {
		log.Fatalf("error creating select statement: %v", err)
	}

	table, err := catalog.GetTable("testdb", "public", "users")
	if err != nil {
		log.Fatalf("error getting table: %v", err)
	}

	cursor, err := table.Cursor()
	if err != nil {
		log.Fatalf("error getting cursor: %v", err)
	}
	defer cursor.Close()

	/*cursor, err = cursor.WithFilter(stmt.Where)
	if err != nil {
		log.Fatalf("error getting cursor: %v", err)
	}*/

	cursor, err = cursor.WithGroupBy(stmt.GroupBy, stmt.Aggregations)
	if err != nil {
		log.Fatalf("error getting cursor: %v", err)
	}

	for cursor.Next() {
		record := make(map[string]interface{})
		for _, column := range stmt.Columns {
			value, err := cursor.ScanField(column)
			if err != nil {
				log.Fatalf("error scanning field: %v", err)
			}
			record[column] = value
		}
		log.Infof("Record: %v", record)
	}
}
