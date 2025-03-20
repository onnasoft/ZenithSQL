package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type CreateUserStatement struct {
	Username string `msgpack:"username"` // Nombre de usuario
	Password string `msgpack:"password"` // Contraseña
}

func NewCreateUserStatement(username, password string) (*CreateUserStatement, error) {
	stmt := &CreateUserStatement{
		Username: username,
		Password: password,
	}

	return stmt, nil
}

func (c CreateUserStatement) Protocol() protocol.MessageType {
	return protocol.CreateUser
}

func (c CreateUserStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(c)
}

func (c *CreateUserStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, c)
}

func (c CreateUserStatement) String() string {
	return fmt.Sprintf("CreateUserStatement{Username: %s}", c.Username)
}
