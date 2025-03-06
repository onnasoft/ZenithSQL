package sqlparser

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/onnasoft/sql-parser/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type CreateTableStatement struct {
	TableName string             `msgpack:"table_name"`
	Columns   []ColumnDefinition `msgpack:"columns"`
	Storage   string             `msgpack:"storage"`
}

type ColumnDefinition struct {
	Name         string `msgpack:"name"`
	Type         string `msgpack:"type"`
	Length       int    `msgpack:"length"`
	PrimaryKey   bool   `msgpack:"primary_key"`
	Index        bool   `msgpack:"index"`
	DefaultValue string `msgpack:"default_value"`
}

func (c *CreateTableStatement) Protocol() protocol.MessageType {
	return protocol.CreateTable
}

func (c *CreateTableStatement) ToBytes() ([]byte, error) {
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

func (c *CreateTableStatement) FromBytes(data []byte) error {
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

func (p *Parser) parseCreateTable(sql string) (*CreateTableStatement, error) {
	sql = cleanSQL(sql)

	storage, sql, err := parseStorageOption(sql)
	if err != nil {
		return nil, err
	}

	tableName, columnDefs, err := extractTableNameAndColumns(sql)
	if err != nil {
		return nil, err
	}

	columnStatements, err := parseColumnDefinitions(columnDefs)
	if err != nil {
		return nil, err
	}

	return &CreateTableStatement{
		TableName: tableName,
		Columns:   columnStatements,
		Storage:   storage,
	}, nil
}

func cleanSQL(sql string) string {
	sql = strings.TrimPrefix(strings.ToUpper(sql), "CREATE TABLE")
	return strings.TrimSpace(sql)
}

func parseStorageOption(sql string) (string, string, error) {
	storage := "rows"
	if strings.Contains(sql, "STORAGE =") {
		parts := strings.SplitN(sql, "STORAGE =", 2)
		sql = strings.TrimSpace(parts[0])
		storage = strings.TrimSpace(strings.ToLower(parts[1]))

		storage = strings.TrimSuffix(storage, ";")

		if !isValidStorageOption(storage) {
			return "", "", NewInvalidStorageOptionError(storage)
		}
	}
	return storage, sql, nil
}

func extractTableNameAndColumns(sql string) (string, string, error) {
	parts := strings.SplitN(sql, "(", 2)
	if len(parts) != 2 {
		return "", "", NewInvalidCreateTableFormatError()
	}

	tableName := strings.TrimSpace(parts[0])
	columnDefs := strings.TrimSuffix(strings.TrimSpace(parts[1]), ")")

	return tableName, columnDefs, nil
}

func parseColumnDefinitions(columnDefs string) ([]ColumnDefinition, error) {
	columns := strings.Split(columnDefs, ",")
	columnStatements := make([]ColumnDefinition, 0, len(columns))

	for _, col := range columns {
		col = strings.TrimSpace(col)
		if col == "" {
			continue
		}

		colDef, err := parseColumn(col)
		if err != nil {
			return nil, err
		}

		columnStatements = append(columnStatements, colDef)
	}

	return columnStatements, nil
}

func parseColumn(col string) (ColumnDefinition, error) {
	colParts := strings.Fields(col)
	if len(colParts) < 2 {
		return ColumnDefinition{}, NewInvalidColumnFormatError()
	}

	colName := colParts[0]
	colType, length := parseColumnTypeAndLength(colParts[1])

	if !isValidDataType(colType) {
		return ColumnDefinition{}, NewInvalidDataTypeError(colType)
	}

	primaryKey, index, defaultValue, err := parseColumnOptions(colParts[2:], colType)
	if err != nil {
		return ColumnDefinition{}, err
	}

	return ColumnDefinition{
		Name:         colName,
		Type:         colType,
		Length:       length,
		PrimaryKey:   primaryKey,
		Index:        index,
		DefaultValue: defaultValue,
	}, nil
}

func parseColumnTypeAndLength(typePart string) (string, int) {
	re := regexp.MustCompile(`^([A-Z]+)(\((\d+)\))?$`)
	matches := re.FindStringSubmatch(strings.ToUpper(typePart))

	colType := matches[1]
	length := 0
	if len(matches) > 3 && matches[3] != "" {
		l, err := strconv.Atoi(matches[3])
		if err == nil {
			length = l
		}
	}

	return colType, length
}

func parseColumnOptions(options []string, colType string) (bool, bool, string, error) {
	primaryKey := false
	index := false
	defaultValue := ""

	for i := 0; i < len(options); i++ {
		part := strings.ToUpper(options[i])
		switch part {
		case "PRIMARY":
			if i+1 < len(options) && strings.ToUpper(options[i+1]) == "KEY" {
				primaryKey = true
				if colType != "UUID" {
					return false, false, "", NewInvalidPrimaryKeyTypeError()
				}
				i++
			}
		case "INDEX":
			index = true
		case "DEFAULT":
			if i+1 < len(options) {
				defaultValue = options[i+1]
				i++
			}
		}
	}

	return primaryKey, index, defaultValue, nil
}
