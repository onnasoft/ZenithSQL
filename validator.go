package sqlparser

import (
	"strings"
)

var validDataTypes = map[string]bool{
	"UUID":      true,
	"INT":       true,
	"BIGINT":    true,
	"SMALLINT":  true,
	"DECIMAL":   true,
	"FLOAT":     true,
	"DOUBLE":    true,
	"VARCHAR":   true,
	"CHAR":      true,
	"TEXT":      true,
	"DATE":      true,
	"TIME":      true,
	"DATETIME":  true,
	"TIMESTAMP": true,
	"BOOLEAN":   true,
	"BLOB":      true,
	"VECTOR":    true,
}

func isValidDataType(dataType string) bool {
	baseType := strings.Split(dataType, "(")[0]
	baseType = strings.ToUpper(baseType)
	return validDataTypes[baseType]
}

var validStorageOptions = map[string]bool{
	"memory":   true,
	"columnar": true,
	"rows":     true,
}

func isValidStorageOption(storage string) bool {
	storage = strings.ToLower(storage)
	return validStorageOptions[storage]
}
