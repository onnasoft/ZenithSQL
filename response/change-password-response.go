package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type ChangePasswordResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewChangePasswordResponse(success bool, message string) *ChangePasswordResponse {
	return &ChangePasswordResponse{
		Success: success,
		Message: message,
	}
}

func (r *ChangePasswordResponse) IsSuccess() bool {
	return r.Success
}

func (r *ChangePasswordResponse) GetMessage() string {
	return r.Message
}

func (r *ChangePasswordResponse) Protocol() protocol.MessageType {
	return protocol.ChangePassword
}

func (r *ChangePasswordResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *ChangePasswordResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *ChangePasswordResponse) String() string {
	return fmt.Sprintf("ChangePasswordResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
