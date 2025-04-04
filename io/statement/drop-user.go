package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type DropUserStatement struct {
	Username string `msgpack:"username"` // Nombre de usuario
}

func NewDropUserStatement(username string) (*DropUserStatement, error) {
	stmt := &DropUserStatement{
		Username: username,
	}

	return stmt, nil
}

func (d DropUserStatement) Protocol() protocol.MessageType {
	return protocol.DropUser
}

func (d DropUserStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(d)
}

func (d *DropUserStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, d)
}

func (d DropUserStatement) String() string {
	return fmt.Sprintf("DropUserStatement{Username: %s}", d.Username)
}
