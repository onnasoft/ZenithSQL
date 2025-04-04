package statement

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type InsertStatement struct {
	TableName string                 `msgpack:"table_name" valid:"required,alphanumunderscore"`
	Values    map[string]interface{} `msgpack:"values" valid:"required"`
}

func NewInsertStatement(tableName string, values map[string]interface{}) (*InsertStatement, error) {
	stmt := &InsertStatement{
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
