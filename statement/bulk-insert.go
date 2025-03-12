package statement

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type BulkInsertStatement struct {
	TableName string                   `msgpack:"table_name" valid:"required,alphanumunderscore"`
	Rows      []map[string]interface{} `msgpack:"rows" valid:"required"`
}

func NewBulkInsertStatement(tableName string, rows []map[string]interface{}) (*BulkInsertStatement, error) {
	stmt := &BulkInsertStatement{
		TableName: tableName,
		Rows:      rows,
	}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (b BulkInsertStatement) Protocol() protocol.MessageType {
	return protocol.BulkInsert
}

func (b BulkInsertStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(b)
}

func (b *BulkInsertStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, b)
}

func (b BulkInsertStatement) String() string {
	return fmt.Sprintf("BulkInsertStatement{TableName: %s, Rows: %v}", b.TableName, len(b.Rows))
}
