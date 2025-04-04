package statement

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type CreateIndexStatement struct {
	IndexName string   `msgpack:"index_name" valid:"required,alphanumunderscore"`
	TableName string   `msgpack:"table_name" valid:"required,alphanumunderscore"`
	Columns   []string `msgpack:"columns" valid:"required"`
}

func NewCreateIndexStatement(indexName, tableName string, columns []string) (*CreateIndexStatement, error) {
	stmt := &CreateIndexStatement{IndexName: indexName, TableName: tableName, Columns: columns}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (c CreateIndexStatement) Protocol() protocol.MessageType {
	return protocol.CreateIndex
}

func (c CreateIndexStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(c)
}

func (c *CreateIndexStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, c)
}

func (c CreateIndexStatement) String() string {
	return fmt.Sprintf("CreateIndexStatement{IndexName: %s, TableName: %s, Columns: %v}", c.IndexName, c.TableName, c.Columns)
}
