package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type ChangePasswordStatement struct {
	Username    string `msgpack:"username"`     // Nombre de usuario
	OldPassword string `msgpack:"old_password"` // Contraseña actual
	NewPassword string `msgpack:"new_password"` // Nueva contraseña
}

func NewChangePasswordStatement(username, oldPassword, newPassword string) (*ChangePasswordStatement, error) {
	stmt := &ChangePasswordStatement{
		Username:    username,
		OldPassword: oldPassword,
		NewPassword: newPassword,
	}

	return stmt, nil
}

func (c ChangePasswordStatement) Protocol() protocol.MessageType {
	return protocol.ChangePassword
}

func (c ChangePasswordStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(c)
}

func (c *ChangePasswordStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, c)
}

func (c ChangePasswordStatement) String() string {
	return fmt.Sprintf("ChangePasswordStatement{Username: %s}", c.Username)
}
