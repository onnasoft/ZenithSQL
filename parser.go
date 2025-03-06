package sqlparser

import (
	"errors"
	"strings"
)

type Parser struct{}

func NewParser() *Parser {
	return &Parser{}
}

func (p *Parser) Parse(sql string) (Statement, error) {
	sql = strings.TrimSpace(sql)
	if strings.HasPrefix(strings.ToUpper(sql), "CREATE TABLE") {
		return p.parseCreateTable(sql)
	}
	if strings.HasPrefix(strings.ToUpper(sql), "CREATE DATABASE") {
		return p.parseCreateDatabase(sql)
	}
	return nil, errors.New("unsupported SQL statement")
}
