package statement

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/sql-parser/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type CreateDatabaseStatement struct {
	DatabaseName string `msgpack:"database_name" valid:"required,alphanumunderscore"`
}

func NewCreateDatabaseStatement(databaseName string) (*CreateDatabaseStatement, error) {
	stmt := &CreateDatabaseStatement{DatabaseName: databaseName}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (c CreateDatabaseStatement) Protocol() protocol.MessageType {
	return protocol.CreateDatabase
}

func (c CreateDatabaseStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(c)
}

func (c *CreateDatabaseStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, c)
}

func (c CreateDatabaseStatement) String() string {
	return fmt.Sprintf("CreateDatabaseStatement{DatabaseName: %s}", c.DatabaseName)
}
