package parser

import (
	"errors"
	"strings"

	"github.com/onnasoft/ZenithSQL/io/statement"
)

type Parser struct{}

func NewParser() *Parser {
	return &Parser{}
}

func (p *Parser) Parse(sql string) (statement.Statement, error) {
	sql = strings.TrimSpace(sql)
	if strings.HasPrefix(strings.ToUpper(sql), "DROP DATABASE") {
		return p.parseDropDatabase(sql)
	}
	if strings.HasPrefix(strings.ToUpper(sql), "CREATE DATABASE") {
		return p.parseCreateDatabase(sql)
	}

	return nil, errors.New("unsupported SQL statement")
}
