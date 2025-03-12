package statement

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type DropIndexStatement struct {
	IndexName string `msgpack:"index_name" valid:"required,alphanumunderscore"`
	TableName string `msgpack:"table_name" valid:"required,alphanumunderscore"`
}

func NewDropIndexStatement(indexName, tableName string) (*DropIndexStatement, error) {
	stmt := &DropIndexStatement{IndexName: indexName, TableName: tableName}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (d DropIndexStatement) Protocol() protocol.MessageType {
	return protocol.DropIndex
}

func (d DropIndexStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(d)
}

func (d *DropIndexStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, d)
}

func (d DropIndexStatement) String() string {
	return fmt.Sprintf("DropIndexStatement{IndexName: %s, TableName: %s}", d.IndexName, d.TableName)
}
