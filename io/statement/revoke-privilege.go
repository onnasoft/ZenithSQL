package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type RevokePrivilegeStatement struct {
	Username  string `msgpack:"username"`
	Privilege string `msgpack:"privilege"`
}

func NewRevokePrivilegeStatement(username, privilege string) (*RevokePrivilegeStatement, error) {
	stmt := &RevokePrivilegeStatement{
		Username:  username,
		Privilege: privilege,
	}

	return stmt, nil
}

func (r RevokePrivilegeStatement) Protocol() protocol.MessageType {
	return protocol.RevokePrivilege
}

func (r RevokePrivilegeStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *RevokePrivilegeStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r RevokePrivilegeStatement) String() string {
	return fmt.Sprintf("RevokePrivilegeStatement{Username: %s, Privilege: %s}", r.Username, r.Privilege)
}
