package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type BeginTransactionResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewBeginTransactionResponse(success bool, message string) *BeginTransactionResponse {
	return &BeginTransactionResponse{
		Success: success,
		Message: message,
	}
}

func (r *BeginTransactionResponse) IsSuccess() bool {
	return r.Success
}

func (r *BeginTransactionResponse) GetMessage() string {
	return r.Message
}

func (r *BeginTransactionResponse) Protocol() protocol.MessageType {
	return protocol.BeginTransaction
}

func (r *BeginTransactionResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *BeginTransactionResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *BeginTransactionResponse) String() string {
	return fmt.Sprintf("BeginTransactionResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
