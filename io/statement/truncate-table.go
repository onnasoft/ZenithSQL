package statement

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type TruncateTableStatement struct {
	Database  string `msgpack:"database" valid:"required,alphanumunderscore"`
	Schema    string `msgpack:"schema" valid:"required,alphanumunderscore"`
	TableName string `msgpack:"table_name" valid:"required,alphanumunderscore"`
}

func NewTruncateTableStatement(database, schema, tableName string) (*TruncateTableStatement, error) {
	stmt := &TruncateTableStatement{
		Database:  database,
		Schema:    schema,
		TableName: tableName,
	}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (t TruncateTableStatement) Protocol() protocol.MessageType {
	return protocol.TruncateTable
}

func (t TruncateTableStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(t)
}

func (t *TruncateTableStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, t)
}

func (t TruncateTableStatement) String() string {
	return fmt.Sprintf("TruncateTableStatement{TableName: %s}", t.TableName)
}
