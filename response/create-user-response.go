package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type CreateUserResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewCreateUserResponse(success bool, message string) *CreateUserResponse {
	return &CreateUserResponse{
		Success: success,
		Message: message,
	}
}

func (r *CreateUserResponse) IsSuccess() bool {
	return r.Success
}

func (r *CreateUserResponse) GetMessage() string {
	return r.Message
}

func (r *CreateUserResponse) Protocol() protocol.MessageType {
	return protocol.CreateUser
}

func (r *CreateUserResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *CreateUserResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *CreateUserResponse) String() string {
	return fmt.Sprintf("CreateUserResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
