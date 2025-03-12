package statement

import (
	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/sql-parser/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type DropDatabaseStatement struct {
	DatabaseName string `msgpack:"database_name" json:"database_name" valid:"required,alphanumunderscore"`
}

func NewDropDatabaseStatement(databaseName string) (*DropDatabaseStatement, error) {
	stmt := &DropDatabaseStatement{DatabaseName: databaseName}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (d *DropDatabaseStatement) Protocol() protocol.MessageType {
	return protocol.DropDatabase
}

func (d *DropDatabaseStatement) Serialize() ([]byte, error) {
	return msgpack.Marshal(d)
}

func (d *DropDatabaseStatement) Deserialize(data []byte) error {
	return msgpack.Unmarshal(data, d)
}
