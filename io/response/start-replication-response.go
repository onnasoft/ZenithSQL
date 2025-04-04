package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type StartReplicationResponse struct {
	Success bool   `msgpack:"success"` // Indica si la operación fue exitosa
	Message string `msgpack:"message"` // Mensaje descriptivo
}

func NewStartReplicationResponse(success bool, message string) *StartReplicationResponse {
	return &StartReplicationResponse{
		Success: success,
		Message: message,
	}
}

func (r *StartReplicationResponse) IsSuccess() bool {
	return r.Success
}

func (r *StartReplicationResponse) GetMessage() string {
	return r.Message
}

func (r *StartReplicationResponse) Protocol() protocol.MessageType {
	return protocol.StartReplication
}

func (r *StartReplicationResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *StartReplicationResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *StartReplicationResponse) String() string {
	return fmt.Sprintf("StartReplicationResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
