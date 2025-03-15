package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type CreateDatabaseResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewCreateDatabaseResponse(success bool, message string) *CreateDatabaseResponse {
	return &CreateDatabaseResponse{
		Success: success,
		Message: message,
	}
}

func (r *CreateDatabaseResponse) IsSuccess() bool {
	return r.Success
}

func (r *CreateDatabaseResponse) GetMessage() string {
	return r.Message
}

func (r *CreateDatabaseResponse) Protocol() protocol.MessageType {
	return protocol.CreateDatabase
}

func (r *CreateDatabaseResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *CreateDatabaseResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *CreateDatabaseResponse) String() string {
	return fmt.Sprintf("CreateDatabaseResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
