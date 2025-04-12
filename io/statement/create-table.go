package statement

import (
	"errors"
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/onnasoft/ZenithSQL/model/fields"
	"github.com/vmihailenco/msgpack/v5"
)

type CreateTableStatement struct {
	Database   string             `msgpack:"database" json:"database" valid:"required,alphanumunderscore"`
	Schema     string             `msgpack:"schema" json:"schema" valid:"required,alphanumunderscore"`
	TableName  string             `msgpack:"table_name" json:"table_name" valid:"required,alphanumunderscore"`
	FieldsMeta []fields.FieldMeta `msgpack:"fields_meta" json:"fields_meta"`
	Storage    string             `msgpack:"storage" json:"storage"`
}

func NewCreateTableStatement(database, schema, tableName string, columns []fields.FieldMeta, storage string) (*CreateTableStatement, error) {
	stmt := &CreateTableStatement{
		TableName:  tableName,
		FieldsMeta: columns,
		Storage:    storage,
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
	return fmt.Sprintf("CreateTableStatement{TableName: %s, FieldsMeta: %v, Storage: %s}", c.TableName, c.FieldsMeta, c.Storage)
}
