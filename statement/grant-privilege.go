package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type GrantPrivilegeStatement struct {
	Username  string `msgpack:"username"`  // Nombre de usuario
	Privilege string `msgpack:"privilege"` // Privilegio a otorgar
}

func NewGrantPrivilegeStatement(username, privilege string) (*GrantPrivilegeStatement, error) {
	stmt := &GrantPrivilegeStatement{
		Username:  username,
		Privilege: privilege,
	}

	return stmt, nil
}

func (g GrantPrivilegeStatement) Protocol() protocol.MessageType {
	return protocol.GrantPrivilege
}

func (g GrantPrivilegeStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(g)
}

func (g *GrantPrivilegeStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, g)
}

func (g GrantPrivilegeStatement) String() string {
	return fmt.Sprintf("GrantPrivilegeStatement{Username: %s, Privilege: %s}", g.Username, g.Privilege)
}
