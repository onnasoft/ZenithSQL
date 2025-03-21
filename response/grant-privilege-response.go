package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type GrantPrivilegeResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewGrantPrivilegeResponse(success bool, message string) *GrantPrivilegeResponse {
	return &GrantPrivilegeResponse{
		Success: success,
		Message: message,
	}
}

func (r *GrantPrivilegeResponse) IsSuccess() bool {
	return r.Success
}

func (r *GrantPrivilegeResponse) GetMessage() string {
	return r.Message
}

func (r *GrantPrivilegeResponse) Protocol() protocol.MessageType {
	return protocol.GrantPrivilege
}

func (r *GrantPrivilegeResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *GrantPrivilegeResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *GrantPrivilegeResponse) String() string {
	return fmt.Sprintf("GrantPrivilegeResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
