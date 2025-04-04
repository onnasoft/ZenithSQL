package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type UpdateResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewUpdateResponse(success bool, message string) *UpdateResponse {
	return &UpdateResponse{
		Success: success,
		Message: message,
	}
}

func (r *UpdateResponse) IsSuccess() bool {
	return r.Success
}

func (r *UpdateResponse) GetMessage() string {
	return r.Message
}

func (r *UpdateResponse) Protocol() protocol.MessageType {
	return protocol.Update
}

func (r *UpdateResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *UpdateResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *UpdateResponse) String() string {
	return fmt.Sprintf("UpdateResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
