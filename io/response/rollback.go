package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type RollbackResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewRollbackResponse(success bool, message string) *RollbackResponse {
	return &RollbackResponse{
		Success: success,
		Message: message,
	}
}

func (r *RollbackResponse) IsSuccess() bool {
	return r.Success
}

func (r *RollbackResponse) GetMessage() string {
	return r.Message
}

func (r *RollbackResponse) Protocol() protocol.MessageType {
	return protocol.Rollback
}

func (r *RollbackResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *RollbackResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *RollbackResponse) String() string {
	return fmt.Sprintf("RollbackResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
