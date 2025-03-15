package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type ReleaseSavepointResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewReleaseSavepointResponse(success bool, message string) *ReleaseSavepointResponse {
	return &ReleaseSavepointResponse{
		Success: success,
		Message: message,
	}
}

func (r *ReleaseSavepointResponse) IsSuccess() bool {
	return r.Success
}

func (r *ReleaseSavepointResponse) GetMessage() string {
	return r.Message
}

func (r *ReleaseSavepointResponse) Protocol() protocol.MessageType {
	return protocol.ReleaseSavepoint
}

func (r *ReleaseSavepointResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *ReleaseSavepointResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *ReleaseSavepointResponse) String() string {
	return fmt.Sprintf("ReleaseSavepointResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
