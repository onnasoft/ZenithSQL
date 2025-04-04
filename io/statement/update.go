package statement

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type UpdateStatement struct {
	TableName string                 `msgpack:"table_name" valid:"required,alphanumunderscore"`
	Updates   map[string]interface{} `msgpack:"updates" valid:"required"`
	Where     string                 `msgpack:"where"`
}

func NewUpdateStatement(tableName string, updates map[string]interface{}, where string) (*UpdateStatement, error) {
	stmt := &UpdateStatement{
		TableName: tableName,
		Updates:   updates,
		Where:     where,
	}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (u UpdateStatement) Protocol() protocol.MessageType {
	return protocol.Update
}

func (u UpdateStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(u)
}

func (u *UpdateStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, u)
}

func (u UpdateStatement) String() string {
	return fmt.Sprintf("UpdateStatement{TableName: %s, Updates: %v, Where: %s}", u.TableName, u.Updates, u.Where)
}
