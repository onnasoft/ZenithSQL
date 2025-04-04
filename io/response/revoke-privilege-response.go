package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type RevokePrivilegeResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewRevokePrivilegeResponse(success bool, message string) *RevokePrivilegeResponse {
	return &RevokePrivilegeResponse{
		Success: success,
		Message: message,
	}
}

func (r *RevokePrivilegeResponse) IsSuccess() bool {
	return r.Success
}

func (r *RevokePrivilegeResponse) GetMessage() string {
	return r.Message
}

func (r *RevokePrivilegeResponse) Protocol() protocol.MessageType {
	return protocol.RevokePrivilege
}

func (r *RevokePrivilegeResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *RevokePrivilegeResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *RevokePrivilegeResponse) String() string {
	return fmt.Sprintf("RevokePrivilegeResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
