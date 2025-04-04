package statement

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type UpsertStatement struct {
	TableName string                 `msgpack:"table_name" valid:"required,alphanumunderscore"`
	Values    map[string]interface{} `msgpack:"values" valid:"required"`
	UniqueKey string                 `msgpack:"unique_key"`
}

func NewUpsertStatement(tableName string, values map[string]interface{}, uniqueKey string) (*UpsertStatement, error) {
	stmt := &UpsertStatement{
		TableName: tableName,
		Values:    values,
		UniqueKey: uniqueKey,
	}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (u UpsertStatement) Protocol() protocol.MessageType {
	return protocol.Upsert
}

func (u UpsertStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(u)
}

func (u *UpsertStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, u)
}

func (u UpsertStatement) String() string {
	return fmt.Sprintf("UpsertStatement{TableName: %s, Values: %v, UniqueKey: %s}", u.TableName, u.Values, u.UniqueKey)
}
