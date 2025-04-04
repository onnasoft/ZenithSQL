package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type SavepointResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewSavepointResponse(success bool, message string) *SavepointResponse {
	return &SavepointResponse{
		Success: success,
		Message: message,
	}
}

func (r *SavepointResponse) IsSuccess() bool {
	return r.Success
}

func (r *SavepointResponse) GetMessage() string {
	return r.Message
}

func (r *SavepointResponse) Protocol() protocol.MessageType {
	return protocol.Savepoint
}

func (r *SavepointResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *SavepointResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *SavepointResponse) String() string {
	return fmt.Sprintf("SavepointResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
