package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type DropIndexResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewDropIndexResponse(success bool, message string) *DropIndexResponse {
	return &DropIndexResponse{
		Success: success,
		Message: message,
	}
}

func (r *DropIndexResponse) IsSuccess() bool {
	return r.Success
}

func (r *DropIndexResponse) GetMessage() string {
	return r.Message
}

func (r *DropIndexResponse) Protocol() protocol.MessageType {
	return protocol.DropIndex
}

func (r *DropIndexResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *DropIndexResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *DropIndexResponse) String() string {
	return fmt.Sprintf("DropIndexResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
