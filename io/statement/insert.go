package statement

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type InsertStatement struct {
	Database  string                   `valid:"required,alphanumunderscore" msgpack:"database"`
	Schema    string                   `valid:"required,alphanumunderscore" msgpack:"schema"`
	TableName string                   `valid:"required,alphanumunderscore" msgpack:"table_name"`
	Values    []map[string]interface{} `msgpack:"values"`
}

func NewInsertStatement(database, schema, tableName string, values []map[string]interface{}) (*InsertStatement, error) {
	stmt := &InsertStatement{
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

func (i InsertStatement) Protocol() protocol.MessageType {
	return protocol.Insert
}

func (i InsertStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(i)
}

func (i *InsertStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, i)
}

func (i InsertStatement) String() string {
	return fmt.Sprintf("InsertStatement{TableName: %s, Values: %v}", i.TableName, i.Values)
}
