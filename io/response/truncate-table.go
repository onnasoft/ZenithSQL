package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type TruncateTableResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewTruncateTableResponse(success bool, message string) *TruncateTableResponse {
	return &TruncateTableResponse{
		Success: success,
		Message: message,
	}
}

func (r *TruncateTableResponse) IsSuccess() bool {
	return r.Success
}

func (r *TruncateTableResponse) GetMessage() string {
	return r.Message
}

func (r *TruncateTableResponse) Protocol() protocol.MessageType {
	return protocol.TruncateTable
}

func (r *TruncateTableResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *TruncateTableResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *TruncateTableResponse) String() string {
	return fmt.Sprintf("TruncateTableResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
