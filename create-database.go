package sqlparser

import (
	"strings"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/sql-parser/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type CreateDatabaseStatement struct {
	DatabaseName string `msgpack:"database_name" json:"database_name" valid:"required,alphanumunderscore"`
}

func (c *CreateDatabaseStatement) Protocol() protocol.MessageType {
	return protocol.CreateDatabase
}

func (c *CreateDatabaseStatement) ToBytes() ([]byte, error) {
	msgpackBytes, err := msgpack.Marshal(c)
	if err != nil {
		return nil, err
	}

	length := len(msgpackBytes)
	prefixedBytes := make([]byte, 4+length)
	prefixedBytes[0] = byte(length >> 24)
	prefixedBytes[1] = byte(length >> 16)
	prefixedBytes[2] = byte(length >> 8)
	prefixedBytes[3] = byte(length)

	copy(prefixedBytes[4:], msgpackBytes)

	return prefixedBytes, nil
}

func (c *CreateDatabaseStatement) FromBytes(data []byte) error {
	if len(data) < 4 {
		return NewInvalidMessagePackDataError()
	}

	length := int(data[0])<<24 | int(data[1])<<16 | int(data[2])<<8 | int(data[3])

	if len(data[4:]) != length {
		return NewInvalidMessagePackDataError()
	}

	err := msgpack.Unmarshal(data[4:], c)
	if err != nil {
		return err
	}

	return nil
}

func (p *Parser) parseCreateDatabase(sql string) (*CreateDatabaseStatement, error) {
	sql = cleanCreateDatabaseSQL(sql)

	databaseName := strings.TrimSpace(sql)
	if !govalidator.Matches(databaseName, `^[a-zA-Z_][a-zA-Z0-9_]*$`) {
		return nil, NewInvalidDatabaseNameError(databaseName)
	}

	statement := &CreateDatabaseStatement{DatabaseName: databaseName}
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
