package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type DeleteResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewDeleteResponse(success bool, message string) *DeleteResponse {
	return &DeleteResponse{
		Success: success,
		Message: message,
	}
}

func (r *DeleteResponse) IsSuccess() bool {
	return r.Success
}

func (r *DeleteResponse) GetMessage() string {
	return r.Message
}

func (r *DeleteResponse) Protocol() protocol.MessageType {
	return protocol.Delete
}

func (r *DeleteResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *DeleteResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *DeleteResponse) String() string {
	return fmt.Sprintf("DeleteResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
