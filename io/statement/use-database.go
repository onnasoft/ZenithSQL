package statement

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type UseDatabaseStatement struct {
	DatabaseName string `msgpack:"database_name" valid:"required,alphanumunderscore"`
}

func NewUseDatabaseStatement(databaseName string) (*UseDatabaseStatement, error) {
	stmt := &UseDatabaseStatement{
		DatabaseName: databaseName,
	}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (u UseDatabaseStatement) Protocol() protocol.MessageType {
	return protocol.UseDatabase
}

func (u UseDatabaseStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(u)
}

func (u *UseDatabaseStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, u)
}

func (u UseDatabaseStatement) String() string {
	return fmt.Sprintf("UseDatabaseStatement{DatabaseName: %s}", u.DatabaseName)
}
