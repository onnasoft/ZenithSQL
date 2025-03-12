package ZenithSQL

import (
	"strings"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/statement"
)

func (p *Parser) parseCreateDatabase(sql string) (*statement.CreateDatabaseStatement, error) {
	sql = cleanCreateDatabaseSQL(sql)
	databaseName := strings.TrimSpace(sql)

	statement := &statement.CreateDatabaseStatement{DatabaseName: databaseName}
	if _, err := govalidator.ValidateStruct(statement); err != nil {
		return nil, err
	}

	return statement, nil
}

func cleanCreateDatabaseSQL(sql string) string {
	sql = strings.TrimPrefix(strings.ToUpper(sql), "CREATE DATABASE")
	sql = strings.TrimSpace(sql)
	sql = strings.TrimSuffix(sql, ";")
	return sql
}
