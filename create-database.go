package sqlparser

import (
	"regexp"
	"strings"

	"github.com/OnnaSoft/sql-parser/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

// CreateDatabaseStatement represents a CREATE DATABASE statement
type CreateDatabaseStatement struct {
	DatabaseName string `msgpack:"database_name"`
}

func (c *CreateDatabaseStatement) Protocol() protocol.MessageType {
	return protocol.CreateDatabase
}

// Serializes the statement into length-prefixed MessagePack bytes
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

	databaseName, err := extractDatabaseName(sql)
	if err != nil {
		return nil, err
	}

	return &CreateDatabaseStatement{
		DatabaseName: databaseName,
	}, nil
}

func cleanCreateDatabaseSQL(sql string) string {
	sql = strings.TrimPrefix(strings.ToUpper(sql), "CREATE DATABASE")
	sql = strings.TrimSpace(sql)
	sql = strings.TrimSuffix(sql, ";")
	return sql
}

func extractDatabaseName(sql string) (string, error) {
	if sql == "" {
		return "", NewInvalidCreateDatabaseFormatError()
	}

	// Check for valid database name (alphanumeric and underscores)
	isValidName := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`).MatchString
	if !isValidName(sql) {
		return "", NewInvalidDatabaseNameError(sql)
	}

	return sql, nil
}
