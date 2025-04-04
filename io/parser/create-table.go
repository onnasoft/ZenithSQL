package parser

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/onnasoft/ZenithSQL/statement"
)

func (p *Parser) parseCreateTable(sql string) (*statement.CreateTableStatement, error) {
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

	return &statement.CreateTableStatement{
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
			return "", "", statement.NewInvalidStorageOptionError(storage)
		}
	}
	return storage, sql, nil
}

func extractTableNameAndColumns(sql string) (string, string, error) {
	parts := strings.SplitN(sql, "(", 2)
	if len(parts) != 2 {
		return "", "", statement.NewInvalidCreateTableFormatError()
	}

	tableName := strings.TrimSpace(parts[0])
	columnDefs := strings.TrimSuffix(strings.TrimSpace(parts[1]), ")")

	return tableName, columnDefs, nil
}

func parseColumnDefinitions(columnDefs string) ([]statement.ColumnDefinition, error) {
	columns := strings.Split(columnDefs, ",")
	columnStatements := make([]statement.ColumnDefinition, 0, len(columns))

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

func parseColumn(col string) (statement.ColumnDefinition, error) {
	colParts := strings.Fields(col)
	if len(colParts) < 2 {
		return statement.ColumnDefinition{}, statement.NewInvalidColumnFormatError()
	}

	colName := colParts[0]
	colType, length := parseColumnTypeAndLength(colParts[1])

	if !isValidDataType(colType) {
		return statement.ColumnDefinition{}, statement.NewInvalidDataTypeError(colType)
	}

	primaryKey, index, defaultValue, err := parseColumnOptions(colParts[2:], colType)
	if err != nil {
		return statement.ColumnDefinition{}, err
	}

	return statement.ColumnDefinition{
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
					return false, false, "", statement.NewInvalidPrimaryKeyTypeError()
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
