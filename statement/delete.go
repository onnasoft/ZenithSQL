package statement

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type DeleteStatement struct {
	TableName string `msgpack:"table_name" valid:"required,alphanumunderscore"`
	Where     string `msgpack:"where"`
}

func NewDeleteStatement(tableName, where string) (*DeleteStatement, error) {
	stmt := &DeleteStatement{
		TableName: tableName,
		Where:     where,
	}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (d DeleteStatement) Protocol() protocol.MessageType {
	return protocol.Delete
}

func (d DeleteStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(d)
}

func (d *DeleteStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, d)
}

func (d DeleteStatement) String() string {
	return fmt.Sprintf("DeleteStatement{TableName: %s, Where: %s}", d.TableName, d.Where)
}
