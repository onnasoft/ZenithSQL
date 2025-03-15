package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type CreateIndexResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewCreateIndexResponse(success bool, message string) *CreateIndexResponse {
	return &CreateIndexResponse{
		Success: success,
		Message: message,
	}
}

func (r *CreateIndexResponse) IsSuccess() bool {
	return r.Success
}

func (r *CreateIndexResponse) GetMessage() string {
	return r.Message
}

func (r *CreateIndexResponse) Protocol() protocol.MessageType {
	return protocol.CreateIndex
}

func (r *CreateIndexResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *CreateIndexResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *CreateIndexResponse) String() string {
	return fmt.Sprintf("CreateIndexResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
