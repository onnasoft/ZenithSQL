package statement

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type ImportStatement struct {
	Database  string                   `valid:"required,alphanumunderscore" msgpack:"database"`
	Schema    string                   `valid:"required,alphanumunderscore" msgpack:"schema"`
	TableName string                   `valid:"required,alphanumunderscore" msgpack:"table_name"`
	Values    []map[string]interface{} `msgpack:"values"`
}

func NewImportStatement(database, schema, tableName string, values []map[string]interface{}) (*ImportStatement, error) {
	stmt := &ImportStatement{
		Database:  database,
		Schema:    schema,
		TableName: tableName,
		Values:    values,
	}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (i ImportStatement) Protocol() protocol.MessageType {
	return protocol.Insert
}

func (i ImportStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(i)
}

func (i *ImportStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, i)
}

func (i ImportStatement) String() string {
	return fmt.Sprintf("ImportStatement{TableName: %s, Values: %v}", i.TableName, i.Values)
}
