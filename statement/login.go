package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type LoginStatement struct {
	Username string `msgpack:"username"`
	Password string `msgpack:"password"`
}

func NewLoginStatement(username, password string) (*LoginStatement, error) {
	stmt := &LoginStatement{
		Username: username,
		Password: password,
	}

	return stmt, nil
}

func (l LoginStatement) Protocol() protocol.MessageType {
	return protocol.Login
}

func (l LoginStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(l)
}

func (l *LoginStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, l)
}

func (l LoginStatement) String() string {
	return fmt.Sprintf("LoginStatement{Username: %s}", l.Username)
}
