package sqlparser

import (
	"strings"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/sql-parser/statement"
)

func (p *Parser) parseDropDatabase(sql string) (*statement.DropDatabaseStatement, error) {
	sql = cleanDropDatabaseSQL(sql)

	databaseName := strings.TrimSpace(sql)
	if !govalidator.Matches(databaseName, `^[a-zA-Z_][a-zA-Z0-9_]*$`) {
		return nil, statement.NewInvalidDatabaseNameError(databaseName)
	}

	statement := &statement.DropDatabaseStatement{DatabaseName: databaseName}
	if _, err := govalidator.ValidateStruct(statement); err != nil {
		return nil, err
	}

	return statement, nil
}

func cleanDropDatabaseSQL(sql string) string {
	sql = strings.TrimPrefix(strings.ToUpper(sql), "DROP DATABASE")
	sql = strings.TrimSpace(sql)
	sql = strings.TrimSuffix(sql, ";")
	return sql
}
