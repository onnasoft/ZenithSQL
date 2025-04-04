package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type CreateTableResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewCreateTableResponse(success bool, message string) *CreateTableResponse {
	return &CreateTableResponse{
		Success: success,
		Message: message,
	}
}

func (r *CreateTableResponse) IsSuccess() bool {
	return r.Success
}

func (r *CreateTableResponse) GetMessage() string {
	return r.Message
}

func (r *CreateTableResponse) Protocol() protocol.MessageType {
	return protocol.CreateTable
}

func (r *CreateTableResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *CreateTableResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *CreateTableResponse) String() string {
	return fmt.Sprintf("CreateTableResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
