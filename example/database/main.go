package main

import (
	"context"

	"github.com/onnasoft/ZenithSQL/core/executor"
	"github.com/onnasoft/ZenithSQL/io/response"
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
	executor := executor.New(catalog)

	/*
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



		result, ok := executor.Execute(context.Background(), stmt).(*response.SelectResponse)
		if !result.IsSuccess() {
			log.Fatalf("error executing select statement: %v", result.GetMessage())
		}
		if !ok {
			log.Fatalf("error casting response to SelectResponse")
		}

		log.Infof("Select executed successfully: %v", result.Rows)*/

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
	})
	if err != nil {
		log.Fatalf("error creating select statement: %v", err)
	}

	result, ok := executor.Execute(context.Background(), stmt).(*response.SelectResponse)
	if !result.IsSuccess() {
		log.Fatalf("error executing select statement: %v", result.GetMessage())
	}
	if !ok {
		log.Fatalf("error casting response to SelectResponse")
	}

	log.Infof("Select executed successfully: %v", result.Rows)
}
