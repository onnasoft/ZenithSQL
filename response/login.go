package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type LoginResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewLoginResponse(success bool, message string) *LoginResponse {
	return &LoginResponse{
		Success: success,
		Message: message,
	}
}

func (r *LoginResponse) IsSuccess() bool {
	return r.Success
}

func (r *LoginResponse) GetMessage() string {
	return r.Message
}

func (r *LoginResponse) Protocol() protocol.MessageType {
	return protocol.Login
}

func (r *LoginResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *LoginResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *LoginResponse) String() string {
	return fmt.Sprintf("LoginResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
