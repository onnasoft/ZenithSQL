package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type RestoreResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewRestoreResponse(success bool, message string) *RestoreResponse {
	return &RestoreResponse{
		Success: success,
		Message: message,
	}
}

func (r *RestoreResponse) IsSuccess() bool {
	return r.Success
}

func (r *RestoreResponse) GetMessage() string {
	return r.Message
}

func (r *RestoreResponse) Protocol() protocol.MessageType {
	return protocol.Restore
}

func (r *RestoreResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *RestoreResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *RestoreResponse) String() string {
	return fmt.Sprintf("RestoreResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
