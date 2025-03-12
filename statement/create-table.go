package statement

import (
	"errors"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/sql-parser/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type CreateTableStatement struct {
	TableName string             `msgpack:"table_name" json:"table_name" valid:"required,alphanumunderscore"`
	Columns   []ColumnDefinition `msgpack:"columns" json:"columns" valid:"required"`
	Storage   string             `msgpack:"storage" json:"storage"`
}

type ColumnDefinition struct {
	Name         string `msgpack:"name" json:"name" valid:"required,alphanumunderscore"`
	Type         string `msgpack:"type" json:"type" valid:"required"`
	Length       int    `msgpack:"length" json:"length"`
	PrimaryKey   bool   `msgpack:"primary_key" json:"primary_key"`
	Index        bool   `msgpack:"index" json:"index"`
	DefaultValue string `msgpack:"default_value" json:"default_value"`
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

func (c *CreateTableStatement) Serialize() ([]byte, error) {
	return msgpack.Marshal(c)
}

func (c *CreateTableStatement) Deserialize(data []byte) error {
	return msgpack.Unmarshal(data, c)
}
