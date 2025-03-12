package statement

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type TruncateTableStatement struct {
	TableName string `msgpack:"table_name" valid:"required,alphanumunderscore"`
}

func NewTruncateTableStatement(tableName string) (*TruncateTableStatement, error) {
	stmt := &TruncateTableStatement{TableName: tableName}

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
