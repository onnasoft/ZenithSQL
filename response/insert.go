package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type InsertResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewInsertResponse(success bool, message string) *InsertResponse {
	return &InsertResponse{
		Success: success,
		Message: message,
	}
}

func (r *InsertResponse) IsSuccess() bool {
	return r.Success
}

func (r *InsertResponse) GetMessage() string {
	return r.Message
}

func (r *InsertResponse) Protocol() protocol.MessageType {
	return protocol.Insert
}

func (r *InsertResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *InsertResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *InsertResponse) String() string {
	return fmt.Sprintf("InsertResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
