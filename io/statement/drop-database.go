package statement

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/io/protocol"
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

func (d *DropDatabaseStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(d)
}

func (d *DropDatabaseStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, d)
}

func (d *DropDatabaseStatement) String() string {
	return fmt.Sprintf("DropDatabaseStatement{DatabaseName: %s}", d.DatabaseName)
}
