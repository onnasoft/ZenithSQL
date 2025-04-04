package statement

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type RebuildIndexStatement struct {
	TableName string `msgpack:"table_name" valid:"required,alphanumunderscore"`
	IndexName string `msgpack:"index_name" valid:"required,alphanumunderscore"`
}

func NewRebuildIndexStatement(tableName, indexName string) (*RebuildIndexStatement, error) {
	stmt := &RebuildIndexStatement{
		TableName: tableName,
		IndexName: indexName,
	}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (r RebuildIndexStatement) Protocol() protocol.MessageType {
	return protocol.RebuildIndex
}

func (r RebuildIndexStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *RebuildIndexStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r RebuildIndexStatement) String() string {
	return fmt.Sprintf("RebuildIndexStatement{TableName: %s, IndexName: %s}", r.TableName, r.IndexName)
}
