package statement

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type SelectStatement struct {
	TableName string   `msgpack:"table_name" valid:"required,alphanumunderscore"`
	Columns   []string `msgpack:"columns"`
	Where     string   `msgpack:"where"`
}

func NewSelectStatement(tableName string, columns []string, where string) (*SelectStatement, error) {
	stmt := &SelectStatement{
		TableName: tableName,
		Columns:   columns,
		Where:     where,
	}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (s SelectStatement) Protocol() protocol.MessageType {
	return protocol.Select
}

func (s SelectStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(s)
}

func (s *SelectStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, s)
}

func (s SelectStatement) String() string {
	return fmt.Sprintf("SelectStatement{TableName: %s, Columns: %v, Where: %s}", s.TableName, s.Columns, s.Where)
}
