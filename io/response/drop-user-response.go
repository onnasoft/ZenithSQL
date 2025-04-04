package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type DropUserResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewDropUserResponse(success bool, message string) *DropUserResponse {
	return &DropUserResponse{
		Success: success,
		Message: message,
	}
}

func (r *DropUserResponse) IsSuccess() bool {
	return r.Success
}

func (r *DropUserResponse) GetMessage() string {
	return r.Message
}

func (r *DropUserResponse) Protocol() protocol.MessageType {
	return protocol.DropUser
}

func (r *DropUserResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *DropUserResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *DropUserResponse) String() string {
	return fmt.Sprintf("DropUserResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
