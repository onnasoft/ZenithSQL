package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type LogoutStatement struct {
	Username string `msgpack:"username"` // Nombre de usuario
}

func NewLogoutStatement(username string) (*LogoutStatement, error) {
	stmt := &LogoutStatement{
		Username: username,
	}

	return stmt, nil
}

func (l LogoutStatement) Protocol() protocol.MessageType {
	return protocol.Logout
}

func (l LogoutStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(l)
}

func (l *LogoutStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, l)
}

func (l LogoutStatement) String() string {
	return fmt.Sprintf("LogoutStatement{Username: %s}", l.Username)
}
