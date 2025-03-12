package statement

import (
	"errors"
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type CreateTableStatement struct {
	TableName string             `msgpack:"table_name" json:"table_name" valid:"required,alphanumunderscore"`
	Columns   []ColumnDefinition `msgpack:"columns" json:"columns" valid:"required"`
	Storage   string             `msgpack:"storage" json:"storage"`
}

func NewCreateTableStatement(tableName string, columns []ColumnDefinition, storage string) (*CreateTableStatement, error) {
	stmt := &CreateTableStatement{
		TableName: tableName,
		Columns:   columns,
		Storage:   storage,
	}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	if len(columns) == 0 {
		return nil, errors.New("at least one column is required")
	}

	for _, col := range columns {
		if _, err := govalidator.ValidateStruct(col); err != nil {
			return nil, err
		}
	}

	return stmt, nil
}

func (c *CreateTableStatement) Protocol() protocol.MessageType {
	return protocol.CreateTable
}

func (c *CreateTableStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(c)
}

func (c *CreateTableStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, c)
}

func (c *CreateTableStatement) String() string {
	return fmt.Sprintf("CreateTableStatement{TableName: %s, Columns: %v, Storage: %s}", c.TableName, c.Columns, c.Storage)
}
