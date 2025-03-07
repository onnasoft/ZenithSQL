package main

import (
	"fmt"

	"log"

	sqlparser "github.com/onnasoft/sql-parser"
)

func main() {
	parser := sqlparser.NewParser()
	stmt, err := parser.Parse("CREATE DATABASE test")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(stmt.Protocol())
}
