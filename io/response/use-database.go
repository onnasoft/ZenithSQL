package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type UseDatabaseResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewUseDatabaseResponse(success bool, message string) *UseDatabaseResponse {
	return &UseDatabaseResponse{
		Success: success,
		Message: message,
	}
}

func (r *UseDatabaseResponse) IsSuccess() bool {
	return r.Success
}

func (r *UseDatabaseResponse) GetMessage() string {
	return r.Message
}

func (r *UseDatabaseResponse) Protocol() protocol.MessageType {
	return protocol.UseDatabase
}

func (r *UseDatabaseResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *UseDatabaseResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *UseDatabaseResponse) String() string {
	return fmt.Sprintf("UseDatabaseResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
