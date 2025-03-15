package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type DropDatabaseResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewDropDatabaseResponse(success bool, message string) *DropDatabaseResponse {
	return &DropDatabaseResponse{
		Success: success,
		Message: message,
	}
}

func (r *DropDatabaseResponse) IsSuccess() bool {
	return r.Success
}

func (r *DropDatabaseResponse) GetMessage() string {
	return r.Message
}

func (r *DropDatabaseResponse) Protocol() protocol.MessageType {
	return protocol.DropDatabase
}

func (r *DropDatabaseResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *DropDatabaseResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *DropDatabaseResponse) String() string {
	return fmt.Sprintf("DropDatabaseResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
