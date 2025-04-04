package statement

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type AlterTableStatement struct {
	TableName string `msgpack:"table_name" valid:"required,alphanumunderscore"`
	Changes   string `msgpack:"changes" valid:"required"`
}

func NewAlterTableStatement(tableName, changes string) (*AlterTableStatement, error) {
	stmt := &AlterTableStatement{TableName: tableName, Changes: changes}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (a AlterTableStatement) Protocol() protocol.MessageType {
	return protocol.AlterTable
}

func (a AlterTableStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(a)
}

func (a *AlterTableStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, a)
}

func (a AlterTableStatement) String() string {
	return fmt.Sprintf("AlterTableStatement{TableName: %s, Changes: %s}", a.TableName, a.Changes)
}
