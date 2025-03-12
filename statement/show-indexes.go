package statement

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type ShowIndexesStatement struct {
	TableName string `msgpack:"table_name" valid:"required,alphanumunderscore"`
}

func NewShowIndexesStatement(tableName string) (*ShowIndexesStatement, error) {
	stmt := &ShowIndexesStatement{TableName: tableName}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (s ShowIndexesStatement) Protocol() protocol.MessageType {
	return protocol.ShowIndexes
}

func (s ShowIndexesStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(s)
}

func (s *ShowIndexesStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, s)
}

func (s ShowIndexesStatement) String() string {
	return fmt.Sprintf("ShowIndexesStatement{TableName: %s}", s.TableName)
}
